<?php

namespace Wind\Beanstalk;

use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Amp\Socket\SocketConnector;
use Wind\Socket\SimpleTextClient;
use Wind\Utils\ArrayUtil;

/**
 * Wind Framework Beanstalk Client
 */
class Client extends SimpleTextClient
{

	const DEFAULT_PRI = 1024;
	const DEFAULT_TTR = 30;

    /**
     * Server Address
     * @var string
     */
    private $address;

    /**
     * 连接超时时间（秒数）
     * @var int
     */
    private $connectTimeout;

    private $tubeUsed = 'default';
    private $watchTubes = ['default'];

	/**
	 * BeanstalkClient constructor.
	 *
	 * @param string $host
	 * @param int $port
     * @param array $options 连接选项，数据键值对，支持以下选项：
     * (bool) autoReconnect: 是否断线自动重连，默认：否。
     * 自动重连将会自动恢复以往正在发送的命令和动作（主动调用 close 不会）
     * (int) reconnectDelay: 重连间隔秒数，默认：5。
     * 此选项适合生产者使用，生产者可以使用单个链接并发的调用 put 进行放入消息，而不需要在并发 put 时使用多个链接。
     * 对于消费者却不太适用，原因是消费者大部分时间会阻塞在 reserve 状态，多个消费者应该使用多个链接。
     * 对于使用不同的 tube 和 watch tube 时，则不该依赖此选项。
	 */
	public function __construct($host='127.0.0.1', $port=11300, $options=[])
	{
        parent::__construct();

        if (isset($options['autoReconnect'])) {
            $this->autoReconnect = $options['autoReconnect'];
        }

        if (isset($options['reconnectDelay'])) {
            $this->reconnectDelay = $options['reconnectDelay'];
        }

        $this->connectTimeout = $options['connectTimeout'] ?? 5;
        $this->address = "tcp://$host:$port";

        $this->connect();
	}

    protected function createSocket(SocketConnector $connector): Socket
    {
        $connectContext = (new ConnectContext)
            ->withConnectTimeout($this->connectTimeout);

        return $connector->connect($this->address, $connectContext);
    }

    protected function authenticate()
    {
        if ($this->tubeUsed != 'default') {
            $this->useTube($this->tubeUsed);
        }

        $watchDefault = false;

        foreach ($this->watchTubes as $tube) {
            if ($tube == 'default') {
                $watchDefault = true;
            } else {
                $this->watch($tube);
            }
        }

        if (!$watchDefault) {
            $this->ignore('default');
        }
    }

    public function bytes(string $buffer): int
    {
        $headEnding = strpos($buffer, "\r\n");

        if ($headEnding === false) {
            return 128;
        }

        $meta = explode(' ', substr($buffer, 0, $headEnding));

        $bytes = match ($meta[0]) {
            'RESERVED', 'FOUND' => (int)$meta[2],
            'OK' => (int)$meta[1],
            default => -1
        };

        if ($bytes == -1) {
            return 0;
        }

        //head\r\nbytes\r\n
        return $bytes + $headEnding + 4 - strlen($buffer);
    }

    /**
     * 发送命令并接收数据
     *
     * @param string $cmd
     * @param string $checkStatus 确认成功匹配状态
     * @param bool $direct
     * @return array
     */
	protected function send($cmd, $checkStatus=null, $direct=false)
    {
        $command = new Command($cmd, $checkStatus);
        return $this->execute($command, $direct);
    }

    /**
     * 使用放入 Job 时的 Tube
     *
     * 不指定时默认为 default。
     *
     * @param string $tube
     * @return bool
     */
	public function useTube($tube)
	{
        $ret = $this->send(sprintf('use %s', $tube), direct: $this->status == self::STATUS_CONNECTING);
        if ($ret['status'] == 'USING' && $ret['meta'][0] == $tube) {
            $this->tubeUsed = $tube;
            return true;
        } else {
            throw new BeanstalkException($ret['status'].": Use tube $tube failed.");
        }
	}

    /**
     * 监控指定 tube 的消息
     *
     * 默认监控 default 的 tube 消息，调用此方法将添加更多的 tube 到监控中。
     * 可以使用 ignore 来取消指定 tube 的监控，如果连接断开后监控将重置为 default。
     * 如果 autoReconnect 设为 true，则自动重连成功后会继续保持之前的监控。
     *
     * @param string $tube
     * @return int 返回当前监控Tube数量
     */
	public function watch($tube)
	{
        $res = $this->send(sprintf('watch %s', $tube), direct: $this->status == self::STATUS_CONNECTING);

        if ($res['status'] == 'WATCHING') {
            if (!in_array($tube, $this->watchTubes)) {
                $this->watchTubes[] = $tube;
            }
            return $res['meta'][0];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 忽略指定 Tube 的监控
     *
     * 忽略后的 tube 将不再获取其消息，同 watch，当 autoReconnect 为 true 时，则自动重连后将会继续忽略之前的设定。
     *
     * @param string $tube
     * @return int 返回剩余监控 tube 数量
     */
	public function ignore($tube)
	{
        $res = $this->send(sprintf('ignore %s', $tube), direct: $this->status == self::STATUS_CONNECTING);

        if ($res['status'] == 'WATCHING') {
            ArrayUtil::removeElement($this->watchTubes, $tube);
            return $res['meta'][0];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 向队列中存入消息
     *
     * @param string $data 消息内容
     * @param int $pri 消息优先级，数字越小越优先，范围是2^32正整数（0-4,294,967,295)，默认为 1024
     * @param int $delay 消息延迟秒数，默认0为不延迟
     * @param int $ttr 消息处理时间，当消息被 RESERVED 后，超出此时间状态未发生变更，则重新回到 ready 队列，最小值为1
     * @return int 成功返回消息ID
     */
	public function put($data, $pri=self::DEFAULT_PRI, $delay=0, $ttr=self::DEFAULT_TTR)
	{
        $cmd = sprintf("put %d %d %d %d\r\n%s", $pri, $delay, $ttr, strlen($data), $data);
        $res = $this->send($cmd);

        if ($res['status'] == 'INSERTED') {
            return $res['meta'][0];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 取出（预订）消息
     *
     * @param int $timeout 取出消息的超时时间秒数，默认不超时，若设置了时间，当达到时间仍没有消息则返回 TIMED_OUT 异常消息
     * @return array 返回数组包含以下字段，id: 消息ID, body: 消息内容, bytes: 消息内容长度
     */
	public function reserve($timeout=null)
	{
        $cmd = isset($timeout) ? sprintf('reserve-with-timeout %d', $timeout) : 'reserve';
        $res = $this->send($cmd);

        if ($res['status'] == 'RESERVED') {
            list($id, $bytes) = $res['meta'];
            return [
                'id' => $id,
                'body' => $res['body'],
                'bytes' => $bytes
            ];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 删除消息
     *
     * @param int $id
     * @return bool
     */
	public function delete($id)
	{
		$this->send(sprintf('delete %d', $id), 'DELETED');
        return true;
	}

    /**
     * 将消息重新放回 ready 队列
     *
     * @param int $id 消息ID
     * @param int $pri 消息优先级，与 put 一致
     * @param int $delay 消息延迟时间，与 put 一致
     * @return bool
     */
	public function release($id, $pri=self::DEFAULT_PRI, $delay=0)
	{
		$this->send(sprintf('release %d %d %d', $id, $pri, $delay), 'RELEASED');
        return true;
	}

    /**
     * 将消息放入 Buried（失败）队列
     *
     * 放入 buried 队列后的消息可以由 kick 唤醒
     *
     * @param int $id
     * @param int $pri kick 出时的优先级
     * @return void
     */
	public function bury($id, $pri=self::DEFAULT_PRI)
	{
		$this->send(sprintf('bury %d %d', $id, $pri), 'BURIED');
	}

    /**
     * 延续消息处理时间
     *
     * 在处理期间的消息可以通过 touch 延迟 ttr 时间，当调用 touch 后，消息的 ttr 的时间将从头算起。
     *
     * @param int $id
     * @return void
     */
	public function touch($id)
	{
		$this->send(sprintf('touch %d', $id), 'TOUCHED');
	}

    /**
     * 检查指定的消息
     *
     * 获取指定的消息内容，但不会改变消息状态
     *
     * @param int $id
     * @return array 返回内容与 reserve 一致
     */
	public function peek($id)
	{
		return $this->peekRead(sprintf('peek %d', $id));
	}

    /**
     * 检查就绪队列中的下一条消息
     *
     * @return array
     */
	public function peekReady()
	{
		return $this->peekRead('peek-ready');
	}

    /**
     * 检查延迟队列中的下一条消息
     *
     * @return array
     */
	public function peekDelayed()
	{
		return $this->peekRead('peek-delayed');
	}

    /**
     * 检查失败队列中的下一条消息
     *
     * @return array
     */
	public function peekBuried()
	{
		return $this->peekRead('peek-buried');
	}

    /**
     * 读取 peek 相应命令的响应
     *
     * @return array
     */
	protected function peekRead($cmd)
	{
        $res = $this->send($cmd);

        if ($res['status'] == 'FOUND') {
            list($id, $bytes) = $res['meta'];
            return [
                'id' => $id,
                'body' => $res['body'],
                'bytes' => $bytes
            ];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 将失败队列中的消息重新踢致就绪或延迟队列中
     *
     * @param int $bound 踢出的消息数量上限
     * @return int 返回实际踢出的消息数量
     */
	public function kick($bound)
	{
        $res = $this->send(sprintf('kick %d', $bound));

        if ($res['status'] == 'KICKED') {
            return $res['meta'][0];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 将指定的消息踢出到就绪或延迟队列中
     *
     * @param int $id
     * @return bool
     */
	public function kickJob($id)
	{
		$this->send(sprintf('kick-job %d', $id), 'KICKED');
        return true;
	}

    /**
     * 统计消息相关信息
     *
     * @param int $id
     * @return array
     */
	public function statsJob($id)
	{
		return $this->statsRead(sprintf('stats-job %d', $id));
	}

    /**
     * 统计 Tube 相关信息
     *
     * @param string $tube
     * @return array
     */
	public function statsTube($tube)
	{
		return $this->statsRead(sprintf('stats-tube %s', $tube));
	}

    /**
     * 返回服务器相关信息
     *
     * @return array
     */
	public function stats()
	{
		return $this->statsRead('stats');
	}

    /**
     * 列出所在存在的 Tube
     *
     * @return array
     */
	public function listTubes()
	{
		return $this->statsRead('list-tubes');
	}

    /**
     * 检查当前客户端正在使用的 tube
     *
     * @return string
     */
	public function listTubeUsed()
	{
        $res = $this->send('list-tube-used');
        if ($res['status'] == 'USING') {
            return $res['meta'][0];
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 列出当前监控的 tube
     *
     * @return array
     */
	public function listTubesWatched()
	{
		return $this->statsRead('list-tubes-watched');
	}

    /**
     * 读取返回的统计或列表信息
     *
     * @param string $cmd
     * @return array
     */
	protected function statsRead($cmd)
	{
        $res = $this->send($cmd);

        if ($res['status'] == 'OK') {
            $body = rtrim($res['body']);
            $data = array_slice(explode("\n", $body), 1);
            $result = [];

            foreach ($data as $row) {
                if ($row[0] == '-') {
                    $value = substr($row, 2);
                    $key = null;
                } else {
                    $pos = strpos($row, ':');
                    $key = substr($row, 0, $pos);
                    $value = substr($row, $pos+2);
                }
                if (is_numeric($value)) {
                    $value = (int)$value == $value ? (int)$value : (float)$value;
                }
                isset($key) ? $result[$key] = $value : array_push($result, $value);
            }
            return $result;
        } else {
            throw new BeanstalkException($res['status']);
        }
	}

    /**
     * 暂停指定 Tube 的消息分发直至指定的延迟时间
     *
     * @param string $tube 要暂停的 Tube
     * @param int $delay 延迟时间秒数
     * @return bool
     */
	public function pauseTube($tube, $delay)
	{
		$this->send(sprintf('pause-tube %s %d', $tube, $delay), 'PAUSED');
        return true;
	}

    protected function cleanResources()
    {
        $this->tubeUsed = 'default';
        $this->watchTubes = ['default'];
    }

    public function __destruct()
    {
        $this->close();
    }
}

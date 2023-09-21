<?php

namespace Wind\Beanstalk;

use Amp\DeferredFuture;
use Amp\Future;
use Throwable;
use Wind\Socket\SimpleTextCommand;

class Command implements SimpleTextCommand
{

    public DeferredFuture $deferred;

    public function __construct(private string $cmd, private ?string $checkStatus=null)
    {
        $this->deferred = new DeferredFuture();
    }

    public function getFuture(): Future
    {
        return $this->deferred->getFuture();
    }

    public function encode(): string
    {
        return $this->cmd."\r\n";
    }

    public function resolve(string|Throwable $buffer)
    {
        if ($buffer instanceof \Throwable) {
            $this->deferred->error($buffer);
            return;
        }

        $headEnding = strpos($buffer, "\r\n");

        if ($headEnding === false) {
            $this->deferred->error(new BeanstalkException('Error buffer format while find head ending \r\n.'));
            return 128;
        }

        $meta = explode(' ', substr($buffer, 0, $headEnding));
        $status = array_shift($meta);

        if ($this->checkStatus !== null && $status != $this->checkStatus) {
            $this->deferred->error(new BeanstalkException($status));
            return;
        }

        $data = [
            'status' => $status,
            'meta' => $meta,
            'body' => strlen($buffer) > $headEnding + 2 ? substr($buffer, $headEnding + 2, -2) : ''
        ];

        $this->deferred->complete($data);
    }

}

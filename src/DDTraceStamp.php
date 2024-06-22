<?php
declare(strict_types=1);

namespace NatePage\DDTrace;

use Symfony\Component\Messenger\Stamp\StampInterface;

final class DDTraceStamp implements StampInterface
{
    private $headers;

    public function __construct(array $headers)
    {
        $this->headers = $headers;
    }

    public function getHeaders(): array
    {
        return $this->headers;
    }
}

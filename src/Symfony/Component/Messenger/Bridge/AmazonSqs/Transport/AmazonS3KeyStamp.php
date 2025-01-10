<?php declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\AmazonSqs\Transport;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class AmazonS3KeyStamp implements NonSendableStampInterface
{
    public function __construct(
        private string $key,
    ) {
    }

    public function getKey(): string
    {
        return $this->key;
    }
}

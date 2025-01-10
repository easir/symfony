<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Bridge\AmazonSqs\Transport;

use AsyncAws\Core\Exception\Http\HttpException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * @author Jérémy Derussé <jeremy@derusse.com>
 */
class AmazonSqsSender implements SenderInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);
        $encodedMessage = $this->complyWithAmazonSqsRequirements($encodedMessage);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = null !== $delayStamp ? (int) ceil($delayStamp->getDelay() / 1000) : 0;

        $messageGroupId = null;
        $messageDeduplicationId = null;

        /** @var AmazonSqsFifoStamp|null $amazonSqsFifoStamp */
        $amazonSqsFifoStamp = $envelope->last(AmazonSqsFifoStamp::class);
        if (null !== $amazonSqsFifoStamp) {
            $messageGroupId = $amazonSqsFifoStamp->getMessageGroupId();
            $messageDeduplicationId = $amazonSqsFifoStamp->getMessageDeduplicationId();
        }

        /** @var AmazonSqsXrayTraceHeaderStamp|null $amazonSqsXrayTraceHeaderStamp */
        $amazonSqsXrayTraceHeaderStamp = $envelope->last(AmazonSqsXrayTraceHeaderStamp::class);
        $xrayTraceId = $amazonSqsXrayTraceHeaderStamp?->getTraceId();

        $s3Key = null;
        if (strlen($encodedMessage['body']) > 256 * 1024) {
            if (false === $this->connection->supportsLargePayload()) {
                throw new TransportException('The message is larger than 256KB to be sent to Amazon SQS, turn on the "large_payload_support" option to enable storing large message in Amazon S3.');
            }

            $s3Key = hash('sha256', $encodedMessage['body'] . microtime(true));
        }

        try {
            $this->connection->send(
                $encodedMessage['body'],
                $encodedMessage['headers'] ?? [],
                $delay,
                $messageGroupId,
                $messageDeduplicationId,
                $xrayTraceId,
                $s3Key,
            );
        } catch (HttpException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }

    /**
     * @see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
     *
     * @param array{body: string, headers?: array<string>} $encodedMessage
     *
     * @return array{body: string, headers?: array<string>}
     */
    private function complyWithAmazonSqsRequirements(array $encodedMessage): array
    {
        if (preg_match('/[^\x20-\x{D7FF}\xA\xD\x9\x{E000}-\x{FFFD}\x{10000}-\x{10FFFF}]/u', $encodedMessage['body'])) {
            $encodedMessage['body'] = base64_encode($encodedMessage['body']);
        }

        return $encodedMessage;
    }
}

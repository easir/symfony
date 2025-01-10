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

use AsyncAws\S3\S3Client;
use AsyncAws\Sqs\Enum\MessageSystemAttributeName;
use AsyncAws\Sqs\Enum\QueueAttributeName;
use AsyncAws\Sqs\Result\ReceiveMessageResult;
use AsyncAws\Sqs\SqsClient;
use AsyncAws\Sqs\ValueObject\MessageAttributeValue;
use AsyncAws\Sqs\ValueObject\MessageSystemAttributeValue;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * @author Jérémy Derussé <jeremy@derusse.com>
 *
 * @internal
 *
 * @final
 */
class Connection
{
    private const AWS_SQS_FIFO_SUFFIX = '.fifo';
    private const MESSAGE_ATTRIBUTE_NAME = 'X-Symfony-Messenger';
    private const S3_CHECKSUM_SHA256_ATTRIBUTE_NAME = 'X-S3-Checksum-Sha256';

    private const DEFAULT_OPTIONS = [
        'buffer_size' => 9,
        'wait_time' => 20,
        'poll_timeout' => 0.1,
        'visibility_timeout' => null,
        'auto_setup' => true,
        'access_key' => null,
        'secret_key' => null,
        'session_token' => null,
        'endpoint' => 'https://sqs.eu-west-1.amazonaws.com',
        'region' => 'eu-west-1',
        'queue_name' => 'messages',
        'account' => null,
        'sslmode' => null,
        'debug' => null,
        'large_payload_support' => null,
    ];

    private array $configuration;
    private SqsClient $client;
    private S3Client $s3Client;
    private ?ReceiveMessageResult $currentResponse = null;
    /** @var array[] */
    private array $buffer = [];

    public function __construct(
        array $configuration,
        ?SqsClient $client = null,
        private ?string $queueUrl = null,
        ?S3Client $s3Client = null,
    ) {
        $this->configuration = array_replace_recursive(self::DEFAULT_OPTIONS, $configuration);
        $this->client = $client ?? new SqsClient([]);
        $this->s3Client = $s3Client ?? new S3Client([]);
    }

    public function __sleep(): array
    {
        throw new \BadMethodCallException('Cannot serialize '.__CLASS__);
    }

    public function __wakeup(): void
    {
        throw new \BadMethodCallException('Cannot unserialize '.__CLASS__);
    }

    public function __destruct()
    {
        $this->reset();
    }

    /**
     * Creates a connection based on the DSN and options.
     *
     * Available options:
     *
     * * endpoint: absolute URL to the SQS service (Default: https://sqs.eu-west-1.amazonaws.com)
     * * region: name of the AWS region (Default: eu-west-1)
     * * queue_name: name of the queue (Default: messages)
     * * account: identifier of the AWS account
     * * access_key: AWS access key
     * * secret_key: AWS secret key
     * * session_token: AWS session token (required only when using temporary credentials)
     * * buffer_size: number of messages to prefetch (Default: 9)
     * * wait_time: long polling duration in seconds (Default: 20)
     * * poll_timeout: amount of seconds the transport should wait for new message
     * * visibility_timeout: amount of seconds the message won't be visible
     * * sslmode: Can be "disable" to use http for a custom endpoint
     * * auto_setup: Whether the queue should be created automatically during send / get (Default: true)
     * * debug: Log all HTTP requests and responses as LoggerInterface::DEBUG (Default: false)
     * * large_payload_support: S3 bucket the transport should use for messages larger than 256KB
     */
    public static function fromDsn(#[\SensitiveParameter] string $dsn, array $options = [], ?HttpClientInterface $client = null, ?LoggerInterface $logger = null): self
    {
        if (false === $params = parse_url($dsn)) {
            throw new InvalidArgumentException('The given Amazon SQS DSN is invalid.');
        }

        $query = [];
        if (isset($params['query'])) {
            parse_str($params['query'], $query);
        }

        // check for extra keys in options
        $optionsExtraKeys = array_diff(array_keys($options), array_keys(self::DEFAULT_OPTIONS));
        if (0 < \count($optionsExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found: [%s]. Allowed options are [%s].', implode(', ', $optionsExtraKeys), implode(', ', array_keys(self::DEFAULT_OPTIONS))));
        }

        // check for extra keys in options
        $queryExtraKeys = array_diff(array_keys($query), array_keys(self::DEFAULT_OPTIONS));
        if (0 < \count($queryExtraKeys)) {
            throw new InvalidArgumentException(\sprintf('Unknown option found in DSN: [%s]. Allowed options are [%s].', implode(', ', $queryExtraKeys), implode(', ', array_keys(self::DEFAULT_OPTIONS))));
        }

        $options = $query + $options + self::DEFAULT_OPTIONS;
        $configuration = [
            'buffer_size' => (int) $options['buffer_size'],
            'wait_time' => (int) $options['wait_time'],
            'poll_timeout' => $options['poll_timeout'],
            'visibility_timeout' => null !== $options['visibility_timeout'] ? (int) $options['visibility_timeout'] : null,
            'auto_setup' => filter_var($options['auto_setup'], \FILTER_VALIDATE_BOOL),
            'queue_name' => (string) $options['queue_name'],
        ];

        $clientConfiguration = [
            'region' => $options['region'],
            'accessKeyId' => rawurldecode($params['user'] ?? '') ?: $options['access_key'] ?? self::DEFAULT_OPTIONS['access_key'],
            'accessKeySecret' => rawurldecode($params['pass'] ?? '') ?: $options['secret_key'] ?? self::DEFAULT_OPTIONS['secret_key'],
        ];
        if (null !== $options['session_token']) {
            $clientConfiguration['sessionToken'] = $options['session_token'];
        }
        if (isset($options['debug'])) {
            $clientConfiguration['debug'] = $options['debug'];
        }
        unset($query['region']);

        $s3Client = new S3Client($clientConfiguration, null, $client, $logger);

        if ('default' !== ($params['host'] ?? 'default')) {
            $clientConfiguration['endpoint'] = \sprintf('%s://%s%s', ($options['sslmode'] ?? null) === 'disable' ? 'http' : 'https', $params['host'], ($params['port'] ?? null) ? ':'.$params['port'] : '');
            if (preg_match(';^sqs\.([^\.]++)\.amazonaws\.com$;', $params['host'], $matches)) {
                $clientConfiguration['region'] = $matches[1];
            }
        } elseif (self::DEFAULT_OPTIONS['endpoint'] !== $options['endpoint'] ?? self::DEFAULT_OPTIONS['endpoint']) {
            $clientConfiguration['endpoint'] = $options['endpoint'];
        }

        $parsedPath = explode('/', ltrim($params['path'] ?? '/', '/'));
        if ($queueName = end($parsedPath)) {
            $configuration['queue_name'] = $queueName;
        }
        $configuration['account'] = 2 === \count($parsedPath) ? $parsedPath[0] : $options['account'] ?? self::DEFAULT_OPTIONS['account'];

        // When the DNS looks like a QueueUrl, we can directly inject it in the connection
        // https://sqs.REGION.amazonaws.com/ACCOUNT/QUEUE
        $queueUrl = null;
        if (
            'https' === $params['scheme']
            && ($params['host'] ?? 'default') === "sqs.{$clientConfiguration['region']}.amazonaws.com"
            && ($params['path'] ?? '/') === "/{$configuration['account']}/{$configuration['queue_name']}"
        ) {
            $queueUrl = 'https://'.$params['host'].$params['path'];
        }

        return new self($configuration, new SqsClient($clientConfiguration, null, $client, $logger), $queueUrl, $s3Client);
    }

    public function get(): ?array
    {
        if ($this->configuration['auto_setup']) {
            $this->setup();
        }

        foreach ($this->getNextMessages() as $message) {
            return $message;
        }

        return null;
    }

    /**
     * @return \Generator<int, array>
     */
    private function getNextMessages(): \Generator
    {
        yield from $this->getPendingMessages();
        yield from $this->getNewMessages();
    }

    /**
     * @return \Generator<int, array>
     */
    private function getPendingMessages(): \Generator
    {
        while ($this->buffer) {
            yield array_shift($this->buffer);
        }
    }

    /**
     * @return \Generator<int, array>
     */
    private function getNewMessages(): \Generator
    {
        if (null === $this->currentResponse) {
            $this->currentResponse = $this->client->receiveMessage([
                'QueueUrl' => $this->getQueueUrl(),
                'VisibilityTimeout' => $this->configuration['visibility_timeout'],
                'MaxNumberOfMessages' => $this->configuration['buffer_size'],
                'MessageAttributeNames' => ['All'],
                'WaitTimeSeconds' => $this->configuration['wait_time'],
            ]);
        }

        if (!$this->fetchMessage()) {
            return;
        }

        yield from $this->getPendingMessages();
    }

    private function fetchMessage(): bool
    {
        if (!$this->currentResponse->resolve($this->configuration['poll_timeout'])) {
            return false;
        }

        foreach ($this->currentResponse->getMessages() as $message) {
            $headers = [];
            $attributes = $message->getMessageAttributes();
            if (isset($attributes[self::MESSAGE_ATTRIBUTE_NAME]) && 'String' === $attributes[self::MESSAGE_ATTRIBUTE_NAME]->getDataType()) {
                $headers = json_decode($attributes[self::MESSAGE_ATTRIBUTE_NAME]->getStringValue(), true);
                unset($attributes[self::MESSAGE_ATTRIBUTE_NAME]);
            }

            $s3Key = null;
            $body = $message->getBody();
            if (isset($attributes[self::S3_CHECKSUM_SHA256_ATTRIBUTE_NAME]) && 'String' === $attributes[self::S3_CHECKSUM_SHA256_ATTRIBUTE_NAME]->getDataType()) {
                $object = $this->s3Client->getObject([
                    'Key' => $body,
                    'Bucket' => $this->configuration['large_payload_support'],
                    'ChecksumSHA256' => $attributes[self::S3_CHECKSUM_SHA256_ATTRIBUTE_NAME]->getStringValue(),
                ]);
                $body = $object->getBody();
            }

            foreach ($attributes as $name => $attribute) {
                if ('String' !== $attribute->getDataType()) {
                    continue;
                }

                $headers[$name] = $attribute->getStringValue();
            }

            $this->buffer[] = [
                'id' => $message->getReceiptHandle(),
                'body' => $body,
                'headers' => $headers,
                's3-key' => $s3Key,
            ];
        }

        $this->currentResponse = null;

        return true;
    }

    public function setup(): void
    {
        // Set to false to disable setup more than once
        $this->configuration['auto_setup'] = false;
        if ($this->client->queueExists([
            'QueueName' => $this->configuration['queue_name'],
            'QueueOwnerAWSAccountId' => $this->configuration['account'],
        ])->isSuccess()) {
            return;
        }

        if (null !== $this->configuration['account']) {
            throw new InvalidArgumentException(\sprintf('The Amazon SQS queue "%s" does not exist (or you don\'t have permissions on it), and can\'t be created when an account is provided.', $this->configuration['queue_name']));
        }

        $parameters = ['QueueName' => $this->configuration['queue_name']];

        if (self::isFifoQueue($this->configuration['queue_name'])) {
            $parameters['Attributes'] = [
                'FifoQueue' => 'true',
            ];
        }

        $this->client->createQueue($parameters);
        $exists = $this->client->queueExists(['QueueName' => $this->configuration['queue_name']]);
        // Blocking call to wait for the queue to be created
        $exists->wait();
        if (!$exists->isSuccess()) {
            throw new TransportException(sprintf('Failed to create the Amazon SQS queue "%s".', $this->configuration['queue_name']));
        }
        $this->queueUrl = null;

        if (null !== $this->configuration['large_payload_support'] && !$this->s3Client->bucketExists($this->configuration['large_payload_support'])) {
            throw new TransportException(sprintf('The Amazon S3 bucket "%s" does not exist.', $this->configuration['large_payload_support']));
        }
    }

    public function delete(string $id, ?string $s3Key): void
    {
        if (null !== $s3Key && null === $this->configuration['large_payload_support']) {
            throw new TransportException('The Amazon S3 bucket is not configured.');
        }

        $this->client->deleteMessage([
            'QueueUrl' => $this->getQueueUrl(),
            'ReceiptHandle' => $id,
        ]);

        if (null === $s3Key) {
            return;
        }

        $this->s3Client->deleteObject([
            'Bucket' => $this->configuration['large_payload_support'],
            'Key' => $s3Key,
        ]);
    }

    /**
     * @param int|null $seconds the minimum duration the message should be kept alive
     */
    public function keepalive(string $id, ?int $seconds = null): void
    {
        $visibilityTimeout = $this->configuration['visibility_timeout'];
        if (null !== $visibilityTimeout && null !== $seconds && $visibilityTimeout < $seconds) {
            throw new TransportException(\sprintf('SQS visibility_timeout (%ds) cannot be smaller than the keepalive interval (%ds).', $visibilityTimeout, $seconds));
        }

        $this->client->changeMessageVisibility([
            'QueueUrl' => $this->getQueueUrl(),
            'ReceiptHandle' => $id,
            'VisibilityTimeout' => $this->configuration['visibility_timeout'],
        ]);
    }

    public function getMessageCount(): int
    {
        $response = $this->client->getQueueAttributes([
            'QueueUrl' => $this->getQueueUrl(),
            'AttributeNames' => [QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES],
        ]);

        $attributes = $response->getAttributes();

        return (int) ($attributes[QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES] ?? 0);
    }

    public function send(string $body, array $headers, int $delay = 0, ?string $messageGroupId = null, ?string $messageDeduplicationId = null, ?string $xrayTraceId = null, ?string $s3Key = null): void
    {
        if ($this->configuration['auto_setup']) {
            $this->setup();
        }

        $parameters = [
            'QueueUrl' => $this->getQueueUrl(),
            'MessageBody' => $body,
            // Maximum delay is 15 minutes. See https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-timers.html.
            'DelaySeconds' => min(900, $delay),
            'MessageAttributes' => [],
            'MessageSystemAttributes' => [],
        ];

        $specialHeaders = [];
        foreach ($headers as $name => $value) {
            if ('.' === $name[0] || self::MESSAGE_ATTRIBUTE_NAME === $name || \strlen($name) > 256 || str_ends_with($name, '.') || str_starts_with($name, 'AWS.') || str_starts_with($name, 'Amazon.') || preg_match('/([^a-zA-Z0-9_\.-]+|\.\.)/', $name)) {
                $specialHeaders[$name] = $value;

                continue;
            }

            $parameters['MessageAttributes'][$name] = new MessageAttributeValue([
                'DataType' => 'String',
                'StringValue' => $value,
            ]);
        }

        if ($specialHeaders) {
            $parameters['MessageAttributes'][self::MESSAGE_ATTRIBUTE_NAME] = new MessageAttributeValue([
                'DataType' => 'String',
                'StringValue' => json_encode($specialHeaders),
            ]);
        }

        if (null !== $xrayTraceId) {
            $parameters['MessageSystemAttributes'][MessageSystemAttributeName::AWSTRACE_HEADER] = new MessageSystemAttributeValue([
                'DataType' => 'String',
                'StringValue' => $xrayTraceId,
            ]);
        }

        if (null !== $this->configuration['large_payload_support'] && null !== $s3Key) {
            $messageSha256 = hash('sha256', $body);
            $parameters['MessageAttributes'][self::S3_CHECKSUM_SHA256_ATTRIBUTE_NAME] = new MessageAttributeValue([
                'DataType' => 'String',
                'StringValue' => $messageSha256,
            ]);
            $this->s3Client->putObject([
                'Bucket' => $this->configuration['large_payload_support'],
                'ChecksumSHA256' => $messageSha256,
                'Key' => $s3Key,
                'Body' => $body,
            ]);
            $parameters['MessageBody'] = $s3Key;
        }

        if (self::isFifoQueue($this->configuration['queue_name'])) {
            $parameters['MessageGroupId'] = $messageGroupId ?? __METHOD__;
            $parameters['MessageDeduplicationId'] = $messageDeduplicationId ?? sha1(json_encode(['body' => $body, 'headers' => $headers]));
            unset($parameters['DelaySeconds']);
        }

        $this->client->sendMessage($parameters);
    }

    public function reset(): void
    {
        if (null !== $this->currentResponse) {
            // fetch current response in order to requeue in transit messages
            if (!$this->fetchMessage()) {
                $this->currentResponse->cancel();
                $this->currentResponse = null;
            }
        }

        foreach ($this->getPendingMessages() as $message) {
            $this->client->changeMessageVisibility([
                'QueueUrl' => $this->getQueueUrl(),
                'ReceiptHandle' => $message['id'],
                'VisibilityTimeout' => 0,
            ]);
        }
    }

    private function getQueueUrl(): string
    {
        if (null !== $this->queueUrl) {
            return $this->queueUrl;
        }

        return $this->queueUrl = $this->client->getQueueUrl([
            'QueueName' => $this->configuration['queue_name'],
            'QueueOwnerAWSAccountId' => $this->configuration['account'],
        ])->getQueueUrl();
    }

    private static function isFifoQueue(string $queueName): bool
    {
        return str_ends_with($queueName, self::AWS_SQS_FIFO_SUFFIX);
    }

    public function supportsLargePayload(): bool
    {
        return null !== $this->configuration['large_payload_support'];
    }
}

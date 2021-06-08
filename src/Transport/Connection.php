<?php
declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;

class Connection
{
    private const ARGUMENTS_AS_INTEGER = [
        'write_timeout',
        'write_retries',
        'read_timeout',
        'shutdown_timeout',
    ];
    private const ARGUMENTS_AS_BOOLEAN = [
        'commit_async',
    ];
    private const ARGUMENTS_AS_STRINGS = [
        'log_level',
    ];
    private KafkaFactory $factory;
    private ?KafkaConsumer $consumer = null;
    private ?Producer $producer = null;
    private bool $subscribed = false;
    private array $options = [
        'write_timeout' => 0,
        'write_retries' => 0,
        'read_timeout' => 0,
        'shutdown_timeout' => 0,
        'commit_async' => true,
    ];
    private array $connectionOptions = [
        'host' => null,
        'port' => null,
        'log_level' => '7',
    ];
    private array $topicOptions = [];

    public function __construct(array $connectionOptions = [], array $topicOptions = [], array $options = [], ?KafkaFactory $factory = null)
    {
        if (!\extension_loaded('rdkafka')) {
            throw new LogicException(\sprintf('You cannot use the "%s" as the "rdkafka" extension is not installed.', __CLASS__));
        }

        $this->connectionOptions = \array_replace($this->connectionOptions, $connectionOptions);
        $this->topicOptions = \array_replace($this->topicOptions, $topicOptions);
        $this->options = \array_replace($this->options, $options);
        $this->factory = $factory ?? new KafkaFactory();
    }

    public static function fromDsn(string $dsn, array $options = []): static
    {
        if (false === $parsedUrl = \parse_url($dsn)) {
            throw new InvalidArgumentException(\sprintf('The given Kafka DSN "%s" is invalid.', $dsn));
        }

        $pathParts = isset($parsedUrl['path']) ? \explode('/', \trim($parsedUrl['path'], '/')) : [];
        $topics = \explode(',', $pathParts[0] ?? 'messages');
        \parse_str($parsedUrl['query'] ?? '', $parsedQuery);

        $kafkaOptions = \array_replace_recursive([
            'connection' => [
                'host' => !isset($parsedUrl['host']) || 'default' === $parsedUrl['host'] ? '127.0.0.1' : $parsedUrl['host'],
                'port' => $parsedUrl['port'] ?? 9092,
                'group.id' => \uniqid('', true),
            ],
            'topic' => \array_map(fn(string $topic) => ['name' => $topic], $topics),
        ], $options, $parsedQuery);

        $connectionOptions = self::normalizeOptions($kafkaOptions['connection']);
        $topicOptions = self::normalizeOptions($kafkaOptions['topic']);
        unset($kafkaOptions['connection'], $kafkaOptions['topic']);
        $options = self::normalizeOptions($kafkaOptions);

        return new static($connectionOptions, $topicOptions, $options);
    }

    private static function normalizeOptions(array $options): array
    {
        foreach (self::ARGUMENTS_AS_BOOLEAN as $value) {
            if (isset($options[$value]) && !\is_bool($options[$value])) {
                $options[$value] = (bool) $options[$value];
            }
        }

        foreach (self::ARGUMENTS_AS_INTEGER as $value) {
            if (isset($options[$value]) && !\is_integer($options[$value])) {
                $options[$value] = (int) $options[$value];
            }
        }

        foreach (self::ARGUMENTS_AS_STRINGS as $value) {
            if (isset($options[$value]) && !\is_string($options[$value])) {
                $options[$value] = (string) $options[$value];
            }
        }

        return $options;
    }

    private function consumer(): KafkaConsumer
    {
        return $this->consumer ??= $this->factory->createConsumer($this->connectionOptions);
    }

    private function subscribedConsumer(): KafkaConsumer
    {
        if (!$this->subscribed) {
            $this->consumer()->subscribe(\array_map(fn(array $topic) => $topic['name'], $this->topicOptions));
            $this->subscribed = true;
        }

        return $this->consumer();
    }

    public function producer(): Producer
    {
        return $this->producer ??= $this->factory->createProducer($this->connectionOptions);
    }

    public function get(): ?Message
    {
        if (!$message = $this->subscribedConsumer()->consume($this->options['read_timeout'])) {
            return null;
        }

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $message;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
            case RD_KAFKA_RESP_ERR__TRANSPORT:
                break;
            default:
                throw new TransportException($message->errstr(), $message->err);
        }

        return null;
    }

    public function ack(Message $message): void
    {
        $consumer = $this->consumer();
        $this->options['commit_async'] ? $consumer->commitAsync($message) : $consumer->commit($message);
    }

    public function send(string $body, ?array $headers = null, string $key = null): void
    {
        $producer = $this->producer();
        foreach ($this->topicOptions as $topic) {
            $topic = $this->factory->createTopic($producer, $topic);
            $topic->producev(
                RD_KAFKA_PARTITION_UA,
                0,
                $body,
                $key,
                $headers
            );
        }
        $producer->poll(0);
    }
}
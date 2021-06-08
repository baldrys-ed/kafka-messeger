<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaSender implements SenderInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer
    )
    {
    }

    public function send(Envelope $envelope): Envelope
    {
        $payload = $this->serializer->encode($envelope);
        $this->connection->send($payload['body'], $payload['headers'], $payload['key'] ?? null);

        return $envelope;
    }
}

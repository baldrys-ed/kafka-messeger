<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use Symfony\Component\DependencyInjection\Attribute\Autoconfigure;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

#[Autoconfigure(tags: ['messenger.transport_factory'])]
class KafkaTransportFactory implements TransportFactoryInterface
{
    private const DSN = 'kafka://';

    public function supports(string $dsn, array $options): bool
    {
        return \str_starts_with($dsn, self::DSN);
    }

    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        return new KafkaTransport(Connection::fromDsn($dsn, $options), $serializer);
    }
}

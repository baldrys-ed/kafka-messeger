<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaReceiver implements ReceiverInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer
    )
    {
    }

    public function get(): iterable
    {
        yield from $this->getEnvelope();
    }

    private function getEnvelope(): iterable
    {
        try {
            $message = $this->connection->get();
        } catch (\Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $message) {
            return;
        }

        try {
            $envelope = $this->serializer->decode([
                'body' => $message->payload,
                'headers' => $message->headers,
            ]);
        } catch (MessageDecodingFailedException $exception) {
            throw $exception;
        }

        yield $envelope->with(new KafkaReceivedStamp($message));
    }

    public function ack(Envelope $envelope): void
    {
        $stamp = $this->findStamp($envelope);

        try {
            $this->connection->ack($stamp->getMessage());
        } catch (\Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function findStamp(Envelope $envelope): KafkaReceivedStamp
    {
        /** @var KafkaReceivedStamp|null $stamp */
        if (null === $stamp = $envelope->last(KafkaReceivedStamp::class)) {
            throw new LogicException('No "KafkaReceivedStamp" stamp found on the Envelope.');
        }

        return $stamp;
    }

    public function reject(Envelope $envelope): void
    {
    }
}

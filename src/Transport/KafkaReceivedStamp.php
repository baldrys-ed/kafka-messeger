<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use RdKafka\Message;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class KafkaReceivedStamp implements NonSendableStampInterface
{
    public function __construct(private Message $message)
    {
    }

    public function getMessage(): Message
    {
        return $this->message;
    }
}

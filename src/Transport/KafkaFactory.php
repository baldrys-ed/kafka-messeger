<?php
declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Kafka\Transport;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

class KafkaFactory
{
    public function createConsumer(array $options): KafkaConsumer
    {
        return new KafkaConsumer($this->createConnection($options));
    }

    public function createProducer(array $options): Producer
    {
        $producer = new Producer($this->createConnection($options));
        \register_shutdown_function([$producer, 'flush'], $options['shutdown_timeout'] ?? -1);

        return $producer;
    }

    public function createTopic(Producer $producer, array $options): ProducerTopic
    {
        return $producer->newTopic($options['name']);
    }

    private function createConnection(array $options): Conf
    {
        $conf = new Conf();
        $conf->set('bootstrap.servers', \sprintf('%s:%s', $options['host'], $options['port']));
        unset($options['host'], $options['port']);

        foreach ($options as $key => $option) {
            $conf->set($key, $option);
        }

        return $conf;
    }
}
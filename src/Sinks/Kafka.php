<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Sinks;

use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;

class Kafka extends AbstractPlugin
{
    protected ?Producer $producer = null;
    protected ProducerConfig $conf;
    protected array $topics = [];
    protected string $posKey = 'binlog.pos';
    protected array $tables = [];

    public function init(): void
    {
        $config = [];
        parent::init();
        [
            $config['bootstrapServer'],
            $config['updateBrokers'],
            $config['acks']
        ] = ArrayHelper::getValueByArray($this->config, ['dsn', 'updateBrokers', 'acks'], ['updateBrokers' => true, 'acks' => 1]);
        if (empty($dsn)) {
            throw new Exception('dsn must be set!');
        }
        $this->conf = new ProducerConfig($config);
    }

    public function run(Message $msg): void
    {
        if (!$this->producer) {
            $this->producer = new Producer($this->conf);
        }
        $this->producer->send("binlog", json_encode($msg->data));
    }
}

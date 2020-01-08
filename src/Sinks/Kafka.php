<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sinks;


use MySQLReplication\Definitions\ConstEventsNames;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Topic;
use RdKafka\TopicConf;

/**
 * Class Kafka
 * @package Rabbit\DB\Relication\Sinks
 */
class Kafka extends AbstractSingletonPlugin
{
    /** @var Producer */
    protected $producer;
    /** @var ProducerTopic[] */
    protected $topics = [];
    /** @var array */
    protected $topicSet = [];
    /** @var string */
    protected $posKey = 'binlog.pos';
    /** @var array */
    protected $tables = [];

    /**
     * @return mixed|void
     */
    public function init()
    {
        parent::init();
        [
            $dsn,
            $set,
            $this->topicSet
        ] = ArrayHelper::getValueByArray($this->config, ['dsn', 'set', 'topicSet'], null, ['set' => [], 'topicSet' => []]);
        if (empty($dsn)) {
            throw new Exception('dsn must be set!');
        }
        $conf = new Conf();
        $conf->set('bootstrap.servers', is_array($dsn) ? implode(',', $dsn) : $dsn);
        foreach ($set as $key => $value) {
            $conf->set((string)$key, (string)$value);
        }
        $this->producer = new Producer($conf);
    }

    /**
     * @throws \Psr\SimpleCache\InvalidArgumentException
     */
    public function run()
    {
        [$table, $type, $items, $file, $pos] = $this->getInput();
        if (!isset($this->topics[$table])) {
            $topicConf = new TopicConf();
            foreach ($this->topicSet as $key => $value) {
                $topicConf->set((string)$key, (string)$value);
            }
            $topic = $this->producer->newTopic($table, $topicConf);
            $this->topics[$table] = $topic;
        } else {
            $topic = $this->topics[$table];
        }
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode([$table, $type, $items, null, null]));
        $this->cache->set($this->posKey, [$file, $pos]);
    }

}
<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sources;


use Psr\Log\LoggerInterface;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;
use RdKafka\KafkaConsumer;

/**
 * Class Kafka
 * @package Rabbit\DB\Relication\Sources
 */
class Kafka extends AbstractSingletonPlugin
{
    /** @var array */
    protected $tables = [];
    /** @var KafkaConsumer */
    protected $consumer;
    /** @var LoggerInterface */
    protected $logger;
    /** @var float|int */
    protected $timeout = 120 * 1000;
    /** @var float */
    protected $sleep = 0.001;

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        [
            $dsn,
            $set,
            $logger,
            $this->timeout,
            $this->sleep,
            $this->tables
        ] = ArrayHelper::getValueByArray($this->config, ['dsn', 'set', 'logger', 'timeout', 'sleep', 'tables'], null, [
            'set' => [],
            'timeout' => $this->timeout,
            'sleep' => $this->sleep
        ]);
        if (empty($dsn) || empty($this->tables)) {
            throw new Exception('dsn and tables must be set!');
        }
        $conf = new Conf();
        $conf->set('bootstrap.servers', is_array($dsn) ? implode(',', $dsn) : $dsn);
        foreach ($set as $key => $value) {
            $conf->set((string)$key, (string)$value);
        }
        $this->consumer = new KafkaConsumer($conf);
        if (!empty($logger)) {
            $this->logger = getDI($logger);
        }
    }

    /**
     * @throws \Exception
     */
    public function run()
    {
        $consumer->subscribe($this->tables);
        while (true) {
            $message = $consumer->consume(120 * 1000);
            if (!empty($message)) {
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        $output = json_decode($message->payload, true);
                        [$table, $type, $_, $file, $pos] = $output;
                        $this->output($output, array_search($table, $this->tables));
                        $consumer->commitAsync();
                        $msg = "success consumer $table $file $pos";
                        $method = 'info';
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        $msg = "No more messages; will wait for more\n";
                        $method = 'warning';
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        $msg = "Timed out\n";
                        $method = 'warning';
                        break;
                    default:
                        throw new \Exception($message->errstr(), $message->err);
                        break;
                }
                if ($this->logger instanceof LoggerInterface) {
                    $this->logger->$method($msg, $this->key);
                }
            }
            \Co::sleep(0.001);
        }
    }


}
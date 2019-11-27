<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sinks;


use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use MySQLReplication\Definitions\ConstEventsNames;
use Psr\SimpleCache\InvalidArgumentException;
use rabbit\App;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\db\clickhouse\BatchInsertJsonRows;
use rabbit\db\clickhouse\Connection;
use rabbit\db\clickhouse\MakeCKConnection;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use Swoole\Timer;

/**
 * Class Clickhouse
 * @package Relication\Sinks
 */
class Clickhouse extends AbstractSingletonPlugin
{
    /** @var Connection */
    protected $db;
    /** @var int */
    protected $bufferSize = 1000;
    /** @var array */
    protected $buffer = [];
    /** @var array */
    protected $tables = [];
    /** @var int */
    private $total = 0;
    /** @var string */
    private $posKey = 'binlog.pos';

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        [
            $class,
            $dsn,
            $config,
            $tick,
            $this->bufferSize,
            $this->tables
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'config', 'tick', 'bufferSize', 'tables'],
            null,
            [
                'config' => [],
                'tick' => 0,
                'bufferSize' => 1,
            ]
        );
        if ($dsn === null || $class === null) {
            throw new InvalidConfigException("class, dsn must be set in $this->key");
        }
        $dbName = md5($dsn);
        MakeCKConnection::addConnection($class, $dbName, $dsn, $config);
        $this->db = getDI('clickhouse')->getConnection($dbName);
        $tick > 0 && Timer::tick($tick * 1000, function () {
            $this->trans();
        });
    }

    /**
     * @param $input
     * @throws Exception
     * @throws InvalidArgumentException
     */
    public function run(&$input): void
    {
        [$table, $type, $items, $file, $pos] = $input;
        $this->cache->set($this->posKey, [$file, $pos]);
        $flag = $this->tables[$table]['flag'];
        if ($type === ConstEventsNames::DELETE) {
            foreach ($items as $item) {
                $item += [$flag => 3];
                $this->buffer[$table]['del'][] = $item;
            }
        } else {
            foreach ($items as $item) {
                $item += [$flag => 0];
                $this->buffer[$table]['save'][] = $item;
            }
        }
        $this->total += count($items);
        if ($this->total >= $this->bufferSize) {
            $this->total = 0;
            $this->trans();
        }
    }

    /**
     * @throws Exception
     * @throws InvalidArgumentException
     */
    protected function trans(): void
    {
        if (!empty($this->buffer)) {
            $buffer = $this->buffer;
            array_splice($this->buffer, 0);
            foreach ($buffer as $table => $types) {
                $key = $this->tables[$table]['key'];
                $flag = $this->tables[$table]['flag'];
                if (isset($types['save'])) {
                    $in = $this->save($table, $key, $types['save']);
                    $sql = "ALTER TABLE $table UPDATE $flag=$flag+1 WHERE ($flag=0 or $flag=1) AND $key in ({$in})";
                    $this->db->createCommand($sql)->execute();
                }
                if (isset($types['del'])) {
                    $in = $this->save($table, $key, $types['del']);
                    $sql = "ALTER TABLE $table UPDATE $flag=$flag+1 WHERE $flag=1 AND $key in ({$in})";
                    $this->db->createCommand($sql)->execute();
                }
            }
            [$file, $pos] = $this->cache->get($this->posKey);
            App::info("save binlog file $file and pos $pos", $this->logKey);
            $this->redis->set($this->posKey, \msgpack_pack([$file, $pos]));
        }
    }

    /**
     * @param string $table
     * @param string $key
     * @param array $items
     * @return string
     */
    protected function save(string $table, string $key, array $items): string
    {
        $ids = [];
        $batch = new BatchInsertJsonRows($table, $this->db);
        foreach ($items as $item) {
            $batch->addRow($item);
            $ids[] = $item[$key];
        }
        $batch->execute();
        return implode(',', $ids);
    }
}
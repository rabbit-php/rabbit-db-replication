<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sources;

use rabbit\core\Context;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\db\redis\ActiveRecord;
use rabbit\db\redis\MakeConnection;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\ConnectionInterface;
use Swoole\Timer;

/**
 * Class Redis
 * @package Rabbit\DB\Relication\Sources
 */
class Redis extends AbstractSingletonPlugin
{
    /** @var ConnectionInterface */
    private $db;
    /** @var array */
    protected $tables = [];
    /** @var int */
    protected $tick = 5;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function init()
    {
        parent::init();
        [
            $dsn,
            $class,
            $tick,
            $this->tables
        ] = ArrayHelper::getValueByArray($this->config, ['dsn', 'class', 'tick', 'tables'], null, ['tick' => $this->tick]);
        if (empty($dsn) || empty($this->tables) || empty($tick)) {
            throw new InvalidConfigException("dsn & tick & tables must be set!");
        }
        $dbName = md5($dsn);
        MakeConnection::addConnection($class, $dbName, $dsn);
        $this->db = getDI('redis.manager')->getConnection($dbName);
    }

    public function run()
    {
        $tick = $this->tick > 0 ? $this->tick : 5;
        Timer::tick($tick * 1000, function () {
            foreach ($this->tables as $table => $config) {
                $model = new class($table, $config['key']) extends ActiveRecord {
                    /**
                     *  constructor.
                     * @param string $tableName
                     * @param string $key
                     */
                    public function __construct(string $tableName, string $key)
                    {
                        Context::set(md5(get_called_class() . 'tableName'), $tableName);
                        Context::set(md5(get_called_class() . 'primaryKey'), $key);
                    }

                    /**
                     * @return array
                     */
                    public static function primaryKey(): array
                    {
                        return [Context::get(md5(get_called_class() . 'primaryKey'))];
                    }

                    /**
                     * @return mixed|string
                     */
                    public static function keyPrefix(): string
                    {
                        return Context::get(md5(get_called_class() . 'tableName'));
                    }
                };

                $result = $model::find()->asArray()->all($this->db);
                foreach ($result as $item) {
                    [
                        $type,
                        $file,
                        $pos
                    ] = ArrayHelper::getValueByArray($item, ['type', 'file', 'pos']);
                    unset($item['type'], $item['file'], $item['pos']);
                    $output = [$table, $type, $item, $file, $pos];
                    $this->output($output);
                }
                $model::deleteAll([$config['key'] => ArrayHelper::getColumn($result, $config['key'])]);
            }
        });
    }

}
<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sinks;


use rabbit\core\Context;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\db\redis\ActiveRecord;
use rabbit\db\redis\MakeConnection;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class Redis
 * @package Rabbit\DB\Relication\Sinks
 */
class Redis extends AbstractSingletonPlugin
{
    /** @var string */
    private $dbName;
    /** @var array */
    protected $tables = [];

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
            $this->tables
        ] = ArrayHelper::getValueByArray($this->config, ['dsn', 'class', 'tables']);
        if (empty($dsn) || empty($this->tables)) {
            throw new InvalidConfigException("dsn and tables must be set!");
        }
        $this->dbName = md5($dsn);
        MakeConnection::addConnection($class, $this->dbName, $dsn);
    }

    public function run()
    {
        [$table, $type, $items, $file, $pos] = $this->getInput();
        $key = $this->tables[$table]['key'];
        foreach ($items as $item) {
            $model = new class($table, $this->dbName, $key, array_keys($item)) extends ActiveRecord {
                /** @var array */
                protected $columns = [];

                /**
                 *  constructor.
                 * @param string $tableName
                 * @param string $dbName
                 */
                public function __construct(string $tableName, string $dbName, string $key, array $columns)
                {
                    Context::set(md5(get_called_class() . 'tableName'), $tableName);
                    Context::set(md5(get_called_class() . 'dbName'), $dbName);
                    Context::set(md5(get_called_class() . 'primaryKey'), $key);
                    $this->columns = $columns;
                }

                /**
                 * @return array
                 */
                public static function primaryKey(): array
                {
                    return [Context::get(md5(get_called_class() . 'primaryKey'))];
                }

                /**
                 * @return array
                 */
                public function attributes(): array
                {
                    return array_merge($this->columns, ['type', 'file', 'pos']);
                }

                /**
                 * @return mixed|string
                 */
                public static function keyPrefix(): string
                {
                    return Context::get(md5(get_called_class() . 'tableName'));
                }

                /**
                 * @return \rabbit\db\redis\Connection
                 */
                public static function getDb()
                {
                    return getDI('redis.manager')->get(Context::get(md5(get_called_class() . 'dbName')));
                }
            };
            $exist = $model::findOne($this->tables[$table]['key']);
            if ($exist) {
                $model = $exist;
            }
            $model->load(array_merge($item, ['type' => $type, 'file' => $file, 'pos' => $pos]), '');
            $model->save(false);
        }
    }

}
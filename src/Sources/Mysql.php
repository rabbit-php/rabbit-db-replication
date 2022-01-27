<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Sources;

use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\RowsDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\EventSubscribers;
use MySQLReplication\MySQLReplicationFactory;
use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Relication\Manager\FilePos;
use Rabbit\DB\Relication\Manager\PosManagerInterface;
use Throwable;

/**
 * Class Mysql
 * @package Relication\Sources
 */
class Mysql extends AbstractPlugin
{
    protected MySQLReplicationFactory $binLogStream;
    protected array $tables = [];
    public array $noTables = [];
    protected string $host = '127.0.0.1';
    protected int $port = 3306;
    protected string $user;
    protected string $pass;
    protected int $slaveId = 666;
    protected float $heartBeat = 0.5;
    protected array $database = [];
    private string $posKey = 'binlog.pos';
    protected ?string $prefix = null;
    protected bool $refresh = false;


    public PosManagerInterface $manager;

    public function init(): void
    {
        parent::init();
        [
            $this->host,
            $this->port,
            $this->user,
            $this->pass,
            $this->slaveId,
            $this->heartBeat,
            $this->tables,
            $this->noTables,
            $this->posKey,
            $this->database,
            $manager,
            $this->prefix,
            $this->refresh
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'host',
                'port',
                'user',
                'pass',
                'slaveId',
                'heartBeat',
                'tables',
                'noTables',
                'posKey',
                'database',
                'manager',
                'prefix',
                'refresh'
            ],
            [
                $this->host,
                $this->port,
                '',
                '',
                $this->slaveId,
                $this->heartBeat,
                $this->tables,
                $this->noTables,
                $this->posKey,
                $this->database,
                null,
                null,
                $this->refresh
            ]
        );
        if ($this->prefix === null) {
            throw new InvalidArgumentException("prefix is empty");
        }
        $this->manager = $manager === null ? new FilePos() : getDI($manager);
    }

    public function save(string $value): void
    {
        $this->manager->savePos($this->posKey, $value);
    }

    public function run(Message $msg): void
    {
        try {
            $builder = (new ConfigBuilder())
                ->withUser($this->user)
                ->withHost($this->host)
                ->withPassword($this->pass)
                ->withPort($this->port)
                ->withSlaveId($this->slaveId)
                ->withHeartbeatPeriod($this->heartBeat)
                ->withDatabasesOnly($this->database)
                ->withTablesOnly($this->tables);
            $gtid = $this->manager->getPos($this->posKey, $this->database);
            if ((int)$gtid > 1) {
                $builder->withGtid("{$this->prefix}:1-{$gtid}");
            } elseif ($this->refresh) {
                $builder->withGtid("{$this->prefix}:1");
            }
            $this->binLogStream = new MySQLReplicationFactory(
                $builder->build()
            );
            $event = new class($this, $msg) extends EventSubscribers
            {
                public function __construct(protected Mysql $plugin, protected Message $msg)
                {
                }

                public function onUpdate(UpdateRowsDTO $event): void
                {
                    $this->rowDTO($event);
                }

                public function onDelete(DeleteRowsDTO $event): void
                {
                    $this->rowDTO($event);
                }

                public function onWrite(WriteRowsDTO $event): void
                {
                    $this->rowDTO($event);
                }

                public function rowDTO(RowsDTO $event): void
                {
                    $table = $event->getTableMap()->getTable();
                    $database = $event->getTableMap()->getDatabase();
                    if (!in_array($table, $this->plugin->noTables)) {
                        $msg = clone $this->msg;
                        $msg->opt['gtid'] = $event->getEventInfo()->getBinLogCurrent()->getGtid();
                        $msg->data = [$database, $table, $event->getType(), $event->getValues(), $event->getEventInfo()->getDateTime()];
                        rgo(function () use ($msg): void {
                            $this->plugin->sink($msg);
                            $this->plugin->save($msg->opt['gtid']);
                        });
                    }
                }
            };
            $this->binLogStream->registerSubscriber($event);
            App::debug("start recv binlog with gtid:{$gtid}", $this->key);
            $this->binLogStream->run();
        } catch (Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception), $this->key);
        }
    }
}

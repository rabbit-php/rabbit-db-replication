<?php
declare(strict_types=1);

namespace Rabbit\DB\Relication\Sources;

use Co\Socket;
use Co\System;
use Doctrine\DBAL\DBALException;
use MySQLReplication\BinLog\BinLogException;
use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Config\ConfigException;
use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\EventSubscribers;
use MySQLReplication\Gtid\GtidException;
use MySQLReplication\MySQLReplicationFactory;
use MySQLReplication\Socket\SocketException as SocketExceptionAlias;
use MySQLReplication\Socket\SocketInterface;
use Psr\SimpleCache\InvalidArgumentException;
use rabbit\App;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;

/**
 * Class Mysql
 * @package Relication\Sources
 */
class Mysql extends AbstractSingletonPlugin
{
    /** @var MySQLReplicationFactory */
    protected $binLogStream;
    /** @var array */
    public $tables = [];
    /** @var string */
    protected $host;
    /** @var int */
    protected $port;
    /** @var string */
    protected $user;
    /** @var string */
    protected $pwd;
    /** @var int */
    protected $slaveId;
    /** @var int */
    protected $heartBeat;
    /** @var string */
    private $posKey = 'binlog.pos';

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     */
    public function init()
    {
        parent::init();
        [
            $this->host,
            $this->port,
            $this->user,
            $this->pwd,
            $this->slaveId,
            $this->heartBeat,
            $this->tables,
            $this->posKey
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'host',
                'port',
                'user',
                'password',
                'slaveId',
                'heartBeat',
                'tables',
                'posKey'
            ],
            null,
            [
                'heartBeat' => 2,
                'posKey' => 'binlog.pos'
            ]
        );
        if (!$this->host || !$this->port || !$this->user || !$this->pwd || !$this->slaveId || !$this->tables) {
            throw new InvalidConfigException("host & port & user & password & slaveId & tables must be set!");
        }
    }

    /**
     * @throws BinLogException
     * @throws ConfigException
     * @throws DBALException
     * @throws GtidException
     * @throws SocketExceptionAlias
     */
    protected function makeStream(): void
    {
        $socket = new class implements SocketInterface
        {
            /** @var Socket */
            protected $conn;

            public function isConnected(): bool
            {
                return $this->conn->errCode === 0;
            }

            public function connectToStream(string $host, int $port): void
            {
                $client = new Socket(AF_INET, SOCK_STREAM);
                $reconnectCount = 0;
                while (true) {
                    if (!$client->connect($host, $port, 3)) {
                        $reconnectCount++;
                        if ($reconnectCount >= 3) {
                            $error = sprintf(
                                'Service connect fail error=%s host=%s port=%s',
                                socket_strerror($client->errCode),
                                $host,
                                $port
                            );
                            throw new Exception($error);
                        }
                        System::sleep(1);
                    } else {
                        break;
                    }
                }
                $this->conn = $client;
            }

            public function readFromSocket(int $length): string
            {
                return $this->conn->recvAll($length);
            }

            public function writeToSocket(string $data): void
            {
                $this->conn->sendAll($data);
            }
        };
        $builder = (new ConfigBuilder())
            ->withUser($this->user)
            ->withHost($this->host)
            ->withPassword($this->pwd)
            ->withPort($this->port)
            ->withSlaveId($this->slaveId)
            ->withHeartbeatPeriod($this->heartBeat)
            ->withTablesOnly($this->tables);
        $current = $this->redis->get($this->posKey);
        if ($current) {
            [$file, $pos] = \msgpack_unpack($current);
            if ($file && $pos) {
                $builder->withBinLogPosition($pos)->withBinLogFileName($file);
            }
        }
        $this->binLogStream = new MySQLReplicationFactory(
            $builder->build(),
            null,
            null,
            null,
            $socket
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    public function run()
    {
        try {
            $this->makeStream();
            $event = new class($this) extends EventSubscribers
            {
                /** @var Mysql */
                protected $plugin;

                public function __construct(Mysql $plugin)
                {
                    $this->plugin = $plugin;
                }

                public function onUpdate(UpdateRowsDTO $event): void
                {
                    $current = $event->getEventInfo()->getBinLogCurrent();
                    $table = $event->getTableMap()->getTable();
                    $output = [$table, $event->getType(), ArrayHelper::getColumn($event->getValues(), 'after'), $current->getBinFileName(), $current->getBinLogPosition()];
                    $this->plugin->output($output, (int)array_search($table, $this->plugin->tables));
                }

                public function onDelete(DeleteRowsDTO $event): void
                {
                    $current = $event->getEventInfo()->getBinLogCurrent();
                    $table = $event->getTableMap()->getTable();
                    $output = [$table, $event->getType(), $event->getValues(), $current->getBinFileName(), $current->getBinLogPosition()];
                    $this->plugin->output($output, (int)array_search($table, $this->plugin->tables));
                }

                public function onWrite(WriteRowsDTO $event): void
                {
                    $current = $event->getEventInfo()->getBinLogCurrent();
                    $table = $event->getTableMap()->getTable();
                    $output = [$table, $event->getType(), $event->getValues(), $current->getBinFileName(), $current->getBinLogPosition()];
                    $this->plugin->output($output, (int)array_search($table, $this->plugin->tables));
                }
            };
            $this->binLogStream->registerSubscriber($event);
            App::info("start recv binlog...", $this->key);
            $this->binLogStream->run();
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception), $this->key);
        }
    }
}

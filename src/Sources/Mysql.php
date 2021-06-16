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
use MySQLReplication\Socket\SocketInterface;
use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Relication\Manager\FilePos;
use Rabbit\DB\Relication\Manager\PosManagerInterface;
use RuntimeException;
use Swoole\Coroutine\Socket;
use Throwable;

/**
 * Class Mysql
 * @package Relication\Sources
 */
class Mysql extends AbstractPlugin
{
    protected MySQLReplicationFactory $binLogStream;
    public array $tables = [];
    protected string $host = '127.0.0.1';
    protected int $port = 3306;
    protected string $user;
    protected string $pass;
    protected int $slaveId = 666;
    protected float $heartBeat = 1.0;
    protected array $database = [];
    private string $posKey = 'binlog.pos';
    protected ?string $prefix = null;

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
            $this->posKey,
            $this->database,
            $manager,
            $this->prefix,
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
                'posKey',
                'database',
                'manager',
                'prefix'
            ],
            [
                $this->host,
                $this->port,
                '',
                '',
                $this->slaveId,
                $this->heartBeat,
                $this->tables,
                $this->posKey,
                $this->database,
                null,
                null
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
            $socket = new class implements SocketInterface
            {
                protected Socket $conn;

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
                                throw new RuntimeException($error);
                            }
                            sleep(1);
                        } else {
                            break;
                        }
                    }
                    $this->conn = $client;
                }

                public function readFromSocket(int $length): string
                {
                    return $this->conn->recvAll($length, -1);
                }

                public function writeToSocket(string $data): void
                {
                    $this->conn->sendAll($data);
                }
            };
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
            if ($gtid) {
                $builder->withGtid("{$this->prefix}:1-{$gtid}");
            }
            $this->binLogStream = new MySQLReplicationFactory(
                $builder->build(),
                null,
                null,
                null,
                $socket
            );
            $event = new class($this, $msg) extends EventSubscribers
            {
                protected Mysql $plugin;
                protected Message $msg;

                public function __construct(Mysql $plugin, Message $msg)
                {
                    $this->plugin = $plugin;
                    $this->msg = $msg;
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
                    $msg = clone $this->msg;
                    $msg->opt['gtid'] = $event->getEventInfo()->getBinLogCurrent()->getGtid();
                    $msg->data = [$event->getTableMap()->getDatabase(), $event->getTableMap()->getTable(), $event->getType(), $event->getValues(), $event->getEventInfo()->getDateTime()];
                    rgo(function () use ($msg) {
                        $this->plugin->sink($msg);
                        $this->plugin->save($msg->opt['gtid']);
                    });
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

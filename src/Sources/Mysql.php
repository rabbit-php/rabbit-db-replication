<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Sources;

use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\GTIDLogDTO;
use MySQLReplication\Event\DTO\RowsDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\Event\EventSubscribers;
use MySQLReplication\MySQLReplicationFactory;
use MySQLReplication\Socket\SocketInterface;
use Rabbit\Base\App;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Relication\Manager\File;
use Rabbit\DB\Relication\Manager\IndexInterface;
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
    protected float $heartBeat = 0.0;
    protected array $database = [];
    private string $posKey = 'binlog.pos';

    public IndexInterface $index;

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
            $this->posKey
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
                'database'
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
                $this->database
            ]
        );
        $this->index = new File();
    }

    public function save(string $value): void
    {
        $this->index->saveIndex($this->posKey, $value);
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
            $gtid = $this->index->getIndex($this->posKey);
            if ($gtid) {
                $gtid = implode(',', array_unique(explode(PHP_EOL, rtrim($gtid))));
                $builder->withGtid($gtid);
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
                private string $key = 'binlog.gtid';

                public function __construct(Mysql $plugin, Message $msg)
                {
                    $this->plugin = $plugin;
                    $this->msg = $msg;
                }

                public function onGTID(GTIDLogDTO $event): void
                {
                    Context::set($this->key, $event->getGtid());
                    $this->plugin->save($event->getGtid());
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
                    $msg = clone $this->msg;
                    $gtid = Context::get($this->key);
                    $msg->opt['gtid'] = $gtid;
                    $msg->data = [$table, $event->getType(), $event->getValues()];
                    rgo(function () use ($msg, $gtid) {
                        $this->plugin->sink($msg);
                        $this->plugin->save($gtid);
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

<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Sinks;

use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Click\StreamWrite;

class Clickhouse extends AbstractPlugin
{
    protected StreamWrite $db;
    protected string $key;
    protected int $bufferSize = 1000;

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws Exception
     */
    public function init(): void
    {
        parent::init();
        [
            $this->key,
            $this->bufferSize,
            $table,
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['key', 'bufferSize', 'table'],
            ['click', $this->bufferSize]
        );
        if ($table === null) {
            throw new InvalidArgumentException("table is empty");
        }
        $this->db = new StreamWrite($table, ['gtid', 'table', 'type', 'value'], db: $this->key);
        $this->db->send();
    }

    public function run(Message $msg): void
    {
        [$table, $type, $items] = $msg->data;
        if (!ArrayHelper::isIndexed($items)) {
            $items = [$items];
        }

        $data = [];
        foreach ($items as $item) {
            $data[] = ['gtid' => $msg->opt['gtid'], 'table' => $table, 'type' => $type, 'value' => $item];
        }
        $this->db->write($data);
    }
}

<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Manager;

use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\FileHelper;

class FilePos implements PosManagerInterface
{
    private $fp;

    private string $path;

    public function __construct(string $path = null)
    {
        $this->path = $path ?? '/tmp/binlog';
        FileHelper::createDirectory($this->path);
    }

    public function getPos(string $key, array $dbNames = []): ?string
    {
        $fileName = $this->path . "/$key";
        if (false === $index = @file_get_contents($fileName)) {
            return null;
        }
        return $index;
    }

    public function savePos(string $key, string $value): void
    {
        $this->openFile($key);
        nlock(function () use ($value) {
            flock($this->fp, LOCK_EX);
            fwrite($this->fp, $value);
            flock($this->fp, LOCK_UN);
        });
    }

    private function openFile(string $key)
    {
        if (!$this->fp) {
            $fileName = $this->path . "/$key";
            if (false === $this->fp = share("open.$fileName", function () use ($fileName) {
                return @fopen($fileName, 'w');
            })->result) {
                throw new InvalidArgumentException("Unable to open file: {$key}");
            }
        }
    }
}

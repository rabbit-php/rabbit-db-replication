<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Manager;

interface PosManagerInterface
{
    public function getPos(string $key): ?string;
    public function savePos(string $key, string $value): void;
}

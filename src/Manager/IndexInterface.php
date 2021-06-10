<?php

declare(strict_types=1);

namespace Rabbit\DB\Relication\Manager;

interface IndexInterface
{
    public function getIndex(string $key): ?string;
    public function saveIndex(string $key, string $value): void;
}

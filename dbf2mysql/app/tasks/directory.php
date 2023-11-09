<?php

namespace App\tasks;

use App\services\dbf2mysql;

class directory
{

    private $odbc;
    private $pdo;
    /**
     * Генерирование таблиц из dbf в mysql для БД pdpl
     *
     * @param  PDO $pdo
     * @param  Object $odbc
     * @param  Console $console
     * @param  array $config - массив таблиц генерируемых из dbf
     * @param array $config_child - массив таблиц дочерних генерируемых из mysql
     */
    public function __construct($pdo, $odbc, $console, $config, $config_child)
    {

        try {
            $this->odbc = $odbc;
            $this->pdo = $pdo;

            $import = new dbf2mysql($this->pdo, $this->odbc, $console);
            $import->dropForeign(array_reverse(array_merge($config, $config_child)));
            foreach ($config as $v) {
                $import->dbf_create_adds($v['db'], $v['name'], $v['path'], $v['select'], $v['where'],$v['indexes'], null, null, $v['prefix'], $v['engine']);
            }
        } finally {
            unset($import);
        }
    }
}

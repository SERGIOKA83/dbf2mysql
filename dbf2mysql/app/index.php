<?php

/**
 * 
 * При работе с порталом многие задачи экспортируют свои данные из dbf в mysql. 
 * 
 * @version 0.1.1
 */

use App\services\perfomance;
use App\Generate\GenerateTable;
use App\modification\Modification;
use App\tasks\subordination;
use Exception;

require_once "../dbf2mysql/vendor/autoload.php";
require_once "../config/Console.php";
require_once "../config/Globals.php";

try {

    $console = new Console();
    $perfomance = new perfomance();
    $perfomance->perf_start();
    $odbc = odbc_connect("impdata", "", "") or die("ODBC not isset!");
    $pdo = Globals::getPDOConnection('portal') or die("MYSQL not connect");
    $modification = new Modification();
    $generate = new GenerateTable();


    /**
     * Дерево подчинённости
     */
    list($config, $config_child) = require_once "configs/config_subordination.php";
    new subordination($pdo, $odbc, $console, $config, $config_child);
    $generate->subordination($pdo);
    $modification->subordination($pdo);
    $modification->run($pdo, $config, $config_child);
    $perfomance->perf_end();
} catch (Exception $e) {
    $console->writeln($e);
}

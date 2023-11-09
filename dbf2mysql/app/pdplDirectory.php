<?php

/**
 * Стартер для формирования справочников ПДПЛ, и получения других таблиц для проекта 
 * 
 * Этот таск зависит от данных формируемых в дереве подчинённости поэтому 
 * перед стартом этого таска должен быть выполнен код console.php в tree_subordination
 *  Примечание: console.php выполняется каждый день для построения дерева подчинённости,
 *  и формирования подчинённости используемой в задаче 16971 "Отчёты дело" 
 */

use App\services\perfomance;
use App\Generate\GenerateTable;
use App\modification\Modification;
use App\tasks\directory;

require_once "D:/webbox/Apache/htdocs/dbf2mysql/vendor/autoload.php";
require_once "D:/webbox/Apache/htdocs/config/Console.php";
require_once "D:/webbox/Apache/htdocs/config/Globals.php";


try {

    $console = new Console();
    $perfomance = new perfomance();
	
    $perfomance->perf_start();
    $odbc = odbc_connect("impdata", "", "") or die("ODBC not isset!"); 
    $pdo = Globals::getPDOConnection('portal') or die("MYSQL not connect");
    $modification = new Modification();
    $generate = new GenerateTable();
    /**
     * ПДПЛ
     * 
     * Миграция справочников из dbf в mysql
     * При миграции справочников порядок может иметь значение т.к данные которые нужны для справочника могут отсутствовать
     * по этой причине в GenerateTable::class можно формировать как пул миграций так и отдельные методы для конкретных таблиц 
     * 
     */

    list($config, $config_child) = require_once "configs/config_directory.php";
    new directory($pdo, $odbc, $console, $config, $config_child);
    $generate->directory($pdo); //генерирование общих справочников
    $modification->directory($pdo);
    $generate->directory39($pdo);
	$modification->run($pdo, $config, $config_child);
	$generate->directory16($pdo);
	$generate->sprTarif($pdo);
   // $generate->editing_digest30($pdo);
    $generate->insertData($pdo);
    $perfomance->perf_end();
} catch (\Exception $e) {
    $console->writeln($e);
}

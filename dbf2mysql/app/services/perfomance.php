<?php


namespace App\services;

require_once "D:/webbox/Apache/htdocs/config/Console.php";

use Console;

/**
 * Механизм определения производительности
 * 
 * Механизм  определения скорости зависит от класса Console
 * для нормального вывода кирилицы и вывода сообщения как в консоли так и в браузере
 * 
 * @version 0.0.2
 */
final class perfomance
{
    /**
     * Свойство хранит начало выполнения
     *
     * @var float
     */
    protected $perf_start;
    /**
     * Свойство хранит конец выполнения
     *
     * @var float
     */
    protected $perf_end;
    /**
     * Объект класса Console
     *
     * @var Object
     */
    protected $console;
    /**
     * Метод определяющий начало выполнения
     *
     * @return void
     */
    public function perf_start()
    {

        $this->console = new Console();
        $this->perf_start = microtime(true);
    }

    /**
     * Метод определяющий конец выполнения.
     * В методе содержится вызов private метода show_result
     * @return void
     */
    public function perf_end()
    {
        $this->perf_end = microtime(true);
        $this->show_result();
    }

    /**
     * Вывод результата скорости выполнения и используемой памяти
     *
     * @return void
     */
    private function show_result()
    {
        $this->console->writeln("Время выполнения:" . round($this->perf_end - $this->perf_start, 6) . "с");
        $this->console->writeln("Пиковое значение объёма памяти, выделенное PHP: " . (memory_get_peak_usage(true) / 1024) . "kB");
    }
}
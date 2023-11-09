<?php

namespace App\services;

use PDOException;

/**
 *  Постоянные проблемы с запросами к бд, для определения того сработали запросы или нет необходимо проверять значение используемого метода,
 *  наиболее важная реализация для mysql т.к там больше всего старого и плохого кода.
 * 
 *  
 *  @version 0.0.3
 *   
 */
class PDOmap
{
    /**
     *  Объект фасада логгера
     *
     * @var object
     */
    private $logger;

    public function __construct()
    {
        require_once "D:/home/atlant/config/WrapperLogger.php";
       
        $this->logger = @new \WrapperLogger(0, "config");
    }
    /**
     * Проверка утверждения запроса через pdo
     * Полученный первый параметр метод возвращает без изменений
     * 
     * @param object $v - объект запроса PDO или Exception
     * @param string $query - строка запроса
     * 
     * @return object
     */
    public function PDOAssert($v, $query = "")
    {
        if ("integer" !== gettype($v)) {
            if ("PDOStatement" !== get_class($v)) {
                $this->MessageFalse($query);
            }
        }
        // return $v;
        // if ("integer" !== gettype($v) || ("boolean" == gettype($v) && false == $v)) {
        //     if ("PDOStatement" !== get_class($v)) {
        //         $this->MessageFalse($query);
        //     }
        // }
        return $v;
    }
    /**
     * Метод уведомления о том что в запросе существует ошибка
     *
     * @param string $query
     *
     * @return void
     */
    private function MessageFalse($query = "")
    {
        $pdo = new PDOException();
        $this->logger->error($pdo . " Текст запроса = " . $query);
    }
    /**
     * Логирование других ошибочных событий
     *
     * @param  string $v - сообщение об ошибке
     *
     * @return void
     */
    public function loger($v)
    {
        $this->logger->error($v);
    }
}

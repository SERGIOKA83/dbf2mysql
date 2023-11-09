<?php

namespace App\services;

use App\services\PDOmap;
use Exception;

/**
 * Механизм миграции данных из dbf в mysql
 * 
 * 
 * @todo Почистить код и привести всё в нормальный вид
 * @todo при создании таблиц предусмотреть установку первичных ключей и индексов после создания всех таблиц +
 * 
 * 
 * @version 0.0.5
 *
 */

class dbf2mysql
{
    /**
     * ODBC connect
     *
     * @var object
     */
    private $connect_dbf;
    /**
     * PDO connect
     *
     * @var object
     */
    private $connect_portal;

    /**
     * Объект консольного вывода||web
     *
     * @var object
     */
    private $console;

    /**
     * Объект логирования запросов PDO
     *
     * @var object
     */
    private $PDOmap;
    /**
     * Конструктор объекта для миграции данных из dbf 
     *
     * @param  object $pdo
     * @param  resource $odbc
     */
    public function __construct($pdo, $odbc, $console)
    {
        try {
            $this->connect_portal = $pdo;
            $this->connect_dbf = $odbc;
            $this->console = $console;
            $this->PDOmap = new PDOmap();
        } catch (Exception $e) {
            $this->console->writeln("Поймано исключение : $e");
        }
    }

    /**
     * Создание таблицы mysql из dbf и заполнение таблицы данными
     * @param string $schema - имя схемы mysql
     * @param string $name - имя файла dbf 
     * @param string $dos2win - что-то с кодировкой
     * @param string $not2lowercase - перевод в нижний регистр
     * @param string $prefix - префикс таблицы бд
     * @param string $path - путь к данным dbf
     * @param string $select - sql запрос для генерирования таблиц из dbf чтобы не тащить лишние данные
     * @param string $where - условия sql запроса для выборки
     * @param string $engine - тип движка Mysql для таблицы 
     *
     * @return void
     */
    public function dbf_create_adds($schema, $name, $path, $select, $where, $indexes = [], $dos2win = null, $not2lowercase = null, $prefix = "", $engine)
    {
        $add_index = "";
        $table_indexes = "";
        $m_table = explode(".", strtolower($name));
        $m_table = $prefix.$m_table[0];
        if(!empty($prefix)){
		    $this->console->writeln("Префикс: $prefix");
        }
        $this->console->writeln("Обновление таблицы: $m_table");

        /**
         * Динамическое создание odbc если указан путь к месту хранения dbf
         */
        if (!empty($path)) {
            $this->connect_dbf = odbc_connect("Driver={Microsoft Visual FoxPro Driver};SourceType=DBF;SourceDB=$path", "", "");
        }

        if (empty($select)) {
            $query = "select * from $name";
        } else {
            $query = $select . " $name " . $where . ";";
        }

        $Results = odbc_exec($this->connect_dbf, $query);

        if ($Results) {
            /**
             * Если таблица существует то она должна быть удалена для нового создания и записи данных
             * или произвести удаление данных без удаления таблицы
             */
            $num_fields = odbc_num_fields($Results);

            $this->drop_table($schema, $m_table);

            $query = "SET FOREIGN_KEY_CHECKS=0;CREATE TABLE  $schema.$m_table (";
            /**
             * Формирование полей таблицы и типов полей 
             */
            for ($i = 1; $i <= $num_fields; $i++) {

                $col_name[$i] = odbc_field_name($Results, $i);
                $col_name[$i] = strtolower($col_name[$i]);
                $col_type = strtolower(odbc_field_type($Results, $i));
                $col_length = odbc_field_len($Results, $i);
                $col_scale = odbc_field_scale($Results, $i);
                $col_type_arr[$i] = $col_type[0];

                switch ($col_type[0]) {
                    case 'c':
                        $col_type = "varchar(" . $col_length . ") default NULL";
                        break;
                    case 'i':
                        $col_type = "int(" . $col_length . ") default '0'";
                        if ($col_length > 9) $col_type = "bigint(" . $col_length . ")";
                        break;
                    case 'f':
                        $col_type = "float default '0'";
                        break;
                    case 'y':
                        $col_type = "float default '0'";
                        break;
                    case 'd':
                        $col_type = "date";
                        break;
                    case 't':
                        $col_type = "datetime";
                        break;
                    case 'n':
                        if ($col_scale == 0) {
                            $col_type = "int(" . $col_length . ")";
                            if ($col_length > 9) $col_type = "bigint(" . $col_length . ")";
                            break;
                        } else {
                            $col_type = "decimal(" . $col_length . "," . $col_scale . ")";
                            break;
                        }
                    case 'm':
                        $col_type = "text";
                        break;
                    case 'g':
                        $col_type = "text";
                        break;
                    case 'b':
                        $col_type = "double default '0'";
                        break;
                    case 'l':
                        $col_type = "tinyint(" . $col_length . ") NOT NULL default '0'";
                        break;
                }
                $query .= "  " . $col_name[$i] . " " . $col_type;
                if ($i < $num_fields) $query .= ",";
            } //for

            if (count($indexes)){

                foreach ($indexes as $i) {
      
                    $table_indexes .= ", INDEX($i)";
                    
                }

            }

            $query .= $table_indexes.") ENGINE = $engine;";

            $this->PDOmap->PDOAssert($this->connect_portal->exec($query), $query);
            $num = $this->insert_data_to_mysql($schema, $Results, $m_table, $num_fields, $col_name, $dos2win, $not2lowercase, $col_type_arr);
           
            $this->console->writeln("Таблица $m_table успешно обновилась: столбцов = $num_fields, строк = $num");
        } else {
            //в этом случае будет передан запрос используемый для odbc
            $this->PDOmap->loger($query." ".odbc_errormsg($this->connect_dbf));
            die($this->console->writeln("Запрос получения данных из DBF выполнен с ошибкой \n".$query. "\n".odbc_errormsg($this->connect_dbf)));
        }
    }

    /**
     * Заполнение новой таблицы mysql данными
     * Возвращает количество добавленых строк
     * Фиормирование запроса на дбавление данных осуществляется в 
     * два этапа, сначала формируется строка с именем схемы и таблицы бд а потом
     * формируются данные для добавления в таблицу, после чего запрос выполняется.
     *
     * @param  string $schema - название схемы бд
     * @param  resource $Results - ресурс возвращённый из odbc(данные из dbf)
     * @param  string $m_table - название таблицы сгенерированной в БД
     * @param  int $num_fields - количество полей в таблице БД
     * @param  array $col_name - имена полей 
     * @param  string $dos2win - флаг указывающий кодировку (возможно не нужен)
     * @param  string $not2lowercase - флаг указывающий на приведение строки к нижнему регистру.
     * @param  array $col_type_arr - параметр типа данных поля
     *
     * @return int
     */
    private function insert_data_to_mysql($schema, $Results, $m_table, $num_fields, $col_name, $dos2win, $not2lowercase, $col_type_arr)
    {
        $a_Row = odbc_fetch_array($Results);
        $limit_count = 500; //лимит для сброса данных в mysql
        $limit = 0; //счётчик
        $num = 0; // всего строк
        $insert = "SET FOREIGN_KEY_CHECKS=0;INSERT INTO $schema.$m_table VALUES ";
        $query = ""; //формирование строки с данными для запроса
        while ($a_Row) {
            $num++;
            $limit++;
            $query .= "(";

            for ($i = 1; $i <= $num_fields; $i++) {
                $field_name = $col_name[$i];
                $field_value[$i] = trim($a_Row[$field_name]);
                $str = $field_value[$i];
                /**
                 * Таблиц с флагом dos2win мало и конвертирование делается без условия
                 * @deprecated 0.1
                 * 
                 */
                if ("1" == $dos2win) {
                    $str = iconv("windows-1251", "UTF-8", $str); //convert_cyr_string($str, "a", "w");
                }
                $str = iconv("windows-1251", "UTF-8", $str);

                if ('1' != $not2lowercase) {
                    $str = strtolower($str);
                }
                if ("n" != $col_type_arr[$i]) {
                    $str = "'" . addslashes($str) . "'";
                }
                $query .= "$str";
                if ($i < $num_fields) $query .= ",";
            }

            $query .= "),";
            $a_Row = odbc_fetch_array($Results);

            if ($limit == $limit_count) {
                $request = $insert . substr($query, 0, -1) . ";";
                $this->PDOmap->PDOAssert($this->connect_portal->exec($request), $request);
                $query = "";
                $limit = 0;
            }
        }
        $request = $insert . substr($query, 0, -1) . ";";
        $this->PDOmap->PDOAssert($this->connect_portal->exec($request), $request);
        return $num;
    }

    /**
     * Удаление таблицы из бд если такая таблица существует
     *
     * @param string $schema - имя схемы БД
     * @param string $m_table - имя таблицы
     * @return void
     */
    protected function drop_table($schema, $m_table)
    {
        $sql = "SET FOREIGN_KEY_CHECKS=0;DROP TABLE IF EXISTS $schema.$m_table ;";
        $this->PDOmap->PDOAssert($this->connect_portal->exec($sql), $sql);

    }
    /**
     * Метод удаления внешних ключей из всех таблиц для пересоздания таблиц
     *
     * @param array $config - содержит массивы конфигураций
     *
     * @return void
     */
    public function dropForeign($config)
    {
        foreach ($config as $v) {
            $schema = $v["db"];
            $name = $v["prefix"] . $v["name"];
            if (isset($v["foreignkeys"])) {
                foreach ($v["foreignkeys"] as $val) {
                    $sql = "SET FOREIGN_KEY_CHECKS=0;ALTER TABLE $schema.$name DROP FOREIGN KEY " . $val["fk_name"];
                    $this->PDOmap->PDOAssert($this->connect_portal->exec($sql), $sql);
                }
            }
        }
    }

    /**
     * Зыкрытие соединения после вызова деструктора
     */
    public function __destruct()
    {
        odbc_close($this->connect_dbf);
    }
}

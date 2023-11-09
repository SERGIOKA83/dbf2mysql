<?php

namespace App\modification;

use App\services\PDOmap;
use Exception, PDO;

/**
 * Класс с модификациями для сгенерированных таблиц в БД
 * 
 * @todo по возможности передалать методы на prepare или execute +
 * @todo Убрать лишнее/добавить механизм уведомления о текущем состоянии +-
 * @todo настроить возвращаемые значения/логирование + -
 * @todo добавить проверки параметров 
 * 
 * @version 0.0.4
 */
class Modification
{
    /**
     * PDO соединение
     *
     * @var \PDO
     */
    private $pdo;
    /**
     * Механизм логирования запросов
     *
     * @var PDOmap
     */
    private $PDOmap;

    /**
     * Запуск методов модификации таблиц, добавление первичных ключей,индексов,внешних ключей,генерирование первичного ключа с автоинкрементом. 
     * @param  object $pdo - Объект соединения с БД
     * @param  array $config - конфигурационный массив таблиц
     * @param  array $config_child - конфигурационный массив дочерних таблиц
     */
    public function run($pdo, $config, $config_child)
    {
        $this->PDOmap = new PDOmap();
        $this->pdo = $pdo;
        $config = array_merge($config, $config_child);

        foreach ($config as $v) {
            $this->primary($v['db'], $v['name'], $v['pr_key'], $v['prefix']);
          //  $this->indexes($v['db'], $v['name'], $v['indexes'],  $v['prefix']);
            $this->generateAIcolumn($v['db'], $v['name'], $v['ai'], $v['prefix']);
        }

        foreach ($config as $v) {
            $this->foreign($v['foreignkeys'], $v['db'], $v['prefix'], $v['name']);
        }
    }

    /**
     * Добавление первичного ключа
     *
     * @param  string $pr_key - первичный ключ таблицы
     * @param  string $schema - название схемы БД
     * @param  string $prefix - префикс таблицы 
     * @param  string $name - название таблицы в MySQL
     *
     * @return void
     */
    private function primary($schema, $name, $pr_key = "", $prefix = "")
    {
        try {
            if (!empty($schema) && !empty($name)) {

                if (!empty($pr_key)) {
                    $m_table = $prefix . $name;
                    $query = "SHOW COLUMNS FROM $schema.$m_table";
                    $columnsMeta = $this->pdo->query($query);
                    $columnsMeta = $columnsMeta->fetchAll(PDO::FETCH_ASSOC);
                    //проверка на то что это поле может быть primary_key,в зависимости от результата будет выбран нужный sql
                    foreach ($columnsMeta as $column) {
                        if ($column['Field'] == $pr_key) {
                            $flag = true;
                            $type = $column['Type'];
                        }
                    }
                    if ($flag) {
                        $query = "SET FOREIGN_KEY_CHECKS=0; ALTER TABLE $schema.$m_table CHANGE $pr_key $pr_key $type PRIMARY KEY; ";
                        $this->PDOmap->PDOAssert($this->pdo->exec($query), $query + 1);
                    } else {
                        $query = "SET FOREIGN_KEY_CHECKS=0; ALTER TABLE $schema.$m_table ADD PRIMARY KEY ( $pr_key ); ";
                        $this->PDOmap->PDOAssert($this->pdo->exec($query), $query + 2);
                    }
                }
            } else {
                $this->PDOmap->PDOAssert(0, "Empty variable schema or name");
            }
        } catch (Exception $e) {
            $this->PDOmap->PDOAssert($e, "Modification primary = $query +3");
        }
    }

    /**
     * Метод добавления индексов в таблицу 
     *
     * @param  array $indexes - индексы таблицы
     * @param  string $schema - схема базы данных
     * @param  string $prefix - префикс таблицы
     * @param  string $name - имя таблицы
     *
     *
     * @return void
     */
    private function indexes($schema, $name, $indexes = "",  $prefix = "")
    {
        echo "indexes start /n";
        try {
            if (!empty($indexes)) {
                foreach ($indexes as $v) {
                    $tmp = trim($v);
                    $table = $prefix . $name;
                    $query = "SET FOREIGN_KEY_CHECKS=0;ALTER TABLE $schema.$table ADD INDEX ( $tmp ); ";
                    echo "$query /n";
                    $this->PDOmap->PDOAssert($this->pdo->exec($query), $query);
                }
            }
            echo "indexes end /n";
        } catch (Exception $e) {
            $this->PDOmap->PDOAssert($e, "Modification indexes = $query");
        }
    }
    /**
     * Метод добавления внешнего ключа для таблицы
     *
     * @param  array $foreignkey - внешний ключ
     * @param  string $schema - название схемы БД
     * @param  string $prefix - префикс таблицы
     * @param  string $name - название таблицы
     *
     * @return void
     */
    private function foreign($foreignkey, $schema, $prefix = "", $name)
    {
        try {
            if (!empty($foreignkey)) {
                $table = $prefix . $name;
                foreach ($foreignkey as $v) {
                    $query = "SET FOREIGN_KEY_CHECKS=0; ALTER TABLE $schema.$table ADD CONSTRAINT " . $v["fk_name"] . "
                        FOREIGN KEY (" . $v["fk_col"] . ") REFERENCES $schema." . $v["fk_ref_table"] . "(" . $v["fk_ref_col"] . ")
                        ON DELETE " . $v["action_del"] . "
                        ON UPDATE " . $v["action_upd"] . ";";
                    $this->PDOmap->PDOAssert($this->pdo->exec($query), $query);
                }
            }
        } catch (Exception $e) {
            $this->PDOmap->PDOAssert($e, "Modification foreign = $query");
        }
    }
    /**
     * Вспомогательный метод для модификации дочерних таблиц,
     * различные запросы которые не выносились в конфигурацию
     * 
     * @return void
     */
    private function all()
    {
        $query = [
            "ALTER TABLE dbf.p_a ADD PRIMARY KEY (e);",
            "ALTER TABLE DBF.p_osdalu_child ADD PRIMARY KEY(tn);",
        ];
        foreach ($query as $request) {
            $this->PDOmap->PDOAssert($this->pdo->query($request), $request);
        }
    }
    /**
     * Метод модификаций данных или структуры таблиц для таска nci
     *
     * @param  object $pdo - коннект к бд
     *
     * @return void
     */
    public function nci($pdo)
    {
        $this->PDOmap = new PDOmap();
        $this->pdo = $pdo;
        $query = [
            "UPDATE nci_test.nci_fschtat SET kprofpl=IF((opd!='' AND NOW()>=odprps), opd, kprofpl);",
            "UPDATE nci_test.nci_rs_mzx SET date_start = NULL,date_end = NULL, date_del = NULL,d_modif = NULL
                WHERE date_start = '1899-12-30' or date_end = '1899-12-30' or date_del = '1899-12-30' or d_modif = '1899-12-30'",
            "UPDATE nci_test.nci_bank_korr SET date_start = NULL,date_end = NULL,date_del = NULL,d_modif = NULL,date_del = NULL
                WHERE date_start = '1899-12-30' or date_end = '1899-12-30' or date_del = '1899-12-30'or d_modif = '1899-12-30'",
        ];
        foreach ($query as $request) {
            $this->PDOmap->PDOAssert($this->pdo->query($request), $request);
        }
    }

    public function tb($pdo)
    {
        $this->PDOmap = new PDOmap();
        $this->pdo = $pdo;
        $query = [];
        foreach ($query as $request) {
            $this->PDOmap->PDOAssert($this->pdo->query($request), $request);
        }
    }
    /**
     * Отключение виртуальных ШЕ для ПДПЛ
     * @todo для виртуальных работает но необходимо предусмотреть работу с пустыми ШЕ(должности) +
     * @todo не запускать на боевом дереве пока не поменяются схемы таблиц на pdpl
     * @todo код работает но update выдаёт Exception это потому что для виртуальных ШЕ нет подчинённых
     *  и переопределять некого, запрос конечно лучше доработатьь
     * @todo сделать тесты для проверки
     * @param PDO $pdo
     *
     * @return void
     */
    public function directory($pdo)
    {
        $this->PDOmap = new PDOmap();
        $query = "SELECT v1.boss,v2.id as id_virtual FROM pdpl.tree v1 join pdpl.staff v2 on(v1.staff = v2.id and v2.fm = 'virtual') ";
        $virtualBoss = $pdo->query($query);
        $virtualBoss = $virtualBoss->fetchAll(PDO::FETCH_ASSOC);
        for ($i = 0; $i <= count($virtualBoss); $i++) {
            $res = $pdo->prepare("UPDATE pdpl.tree set boss = :id_boss WHERE boss = :id_virtual");
            $res->bindValue(":id_boss", $virtualBoss[$i]['boss']);
            $res->bindValue(":id_virtual", $virtualBoss[$i]['id_virtual']);
            $res->execute();
        }
        // пустые ше но у них есть подчинённые
        $query = "SELECT v1.boss,v2.id as id_empty FROM pdpl.tree v1 join pdpl.staff v2 on(v1.staff = v2.id and v2.fm = '')";
        $emptyChief = $pdo->query($query);
        $emptyChief = $emptyChief->fetchAll(PDO::FETCH_ASSOC);
        for ($i = 0; $i <= count($emptyChief); $i++) {
            $res = $pdo->prepare("UPDATE pdpl.tree set boss = :id_boss WHERE boss = :id_empty");
            $res->bindValue(":id_boss", $emptyChief[$i]['boss']);
            $res->bindValue(":id_empty", $emptyChief[$i]['id_empty']);
            $res->execute();
        }
        // ше 20-25 lvl но они не проводят оценку
        $query = "SELECT v1.boss,v2.id as id_empty FROM pdpl.tree v1 join pdpl.staff v2 on(v1.staff = v2.id and v2.kod_ur in(20,25))";
        $emptyChief = $pdo->query($query);
        $emptyChief = $emptyChief->fetchAll(PDO::FETCH_ASSOC);
        for ($i = 0; $i <= count($emptyChief); $i++) {
            $res = $pdo->prepare("UPDATE pdpl.tree set boss = :id_boss WHERE boss = :id_empty");
            $res->bindValue(":id_boss", $emptyChief[$i]['boss']);
            $res->bindValue(":id_empty", $emptyChief[$i]['id_empty']);
            $res->execute();
        }
    }
    /**
     * Метод модификаций данных или структуры таблиц для таска nci
     *
     * @param  PDO $pdo - коннект к бд
     *
     * @return void
     */
    public function subordination($pdo)
    {
        $this->PDOmap = new PDOmap();
        $this->pdo = $pdo;
        $query = [
            "ALTER TABLE subordination.staff ADD COLUMN hidden VARCHAR(15) NOT NULL;"
        ];
        foreach ($query as $request) {
            $this->PDOmap->PDOAssert($this->pdo->query($request), $request);
        }
    }

    /**
     * Метод генерирование поля id с автоинкрементом
     * Метод вызывается если в файле конфигурации стоит соответствующая настройка
     *  -ai = id - not empty
     * автоинкрементом останется и повторный запрос приведёт к ошибке
     * @todo добавить проверку на тип поля т.к без проверки это приведёт к ошибке
     * @param  string $schema - имя схемы БД
     * @param  string $name - имя таблицы БД
     * @param  string $ai - название поля с создаваемым ключом 
     * @param  string $prefix - префикс таблицы 
     * @return void
     */
    public function generateAIcolumn($schema, $name, $ai, $prefix = "")
    {
        if (!empty($schema) && !empty($name)) {
            try {
                if (!empty($ai)) {
                    $m_table = $prefix . $name;
                    $query = "SHOW COLUMNS FROM $schema.$m_table";
                    $columnsMeta = $this->pdo->query($query);
                    $columnsMeta = $columnsMeta->fetchAll(PDO::FETCH_ASSOC);
                    //проверка на то что это поле может быть ai,в зависимости от результата будет выбран нужный sql
                    foreach ($columnsMeta as $column) {
                        if ($column['Field'] == $ai && $column['Key'] == 'PRI') {
                            $flag = true;
                        }
                    }
                    if ($flag) {
                        $query = "SET FOREIGN_KEY_CHECKS=0;SET FOREIGN_KEY_CHECKS=0;Alter Table $schema.$m_table CHANGE $ai $ai int(11) AUTO_INCREMENT;";
                    } else {
                        $query = "SET FOREIGN_KEY_CHECKS=0;Alter Table $schema.$m_table ADD COLUMN `$ai` int(11) NOT NULL AUTO_INCREMENT FIRST,ADD PRIMARY KEY (`$ai`)";
                    }
                    $this->PDOmap->PDOAssert($this->pdo->exec($query), $query);
                }
            } catch (Exception $e) {
                $this->PDOmap->PDOAssert($e, "Modification AI =" . $query);
            }
        } else {
            $this->PDOmap->PDOAssert(0, "Empty variable schema or name");
        }
    }

    /**
     * Метод модификаций данных или структуры таблиц для таска nci
     * Task 16092
     * @param  object $pdo - коннект к бд
     *
     * @return void
     */
    public function tasknbrb($pdo)
    {
        $this->PDOmap = new PDOmap();
        $this->pdo = $pdo;
        $query = [
            ""
        ];
        foreach ($query as $request) {
            $this->PDOmap->PDOAssert($this->pdo->query($request), $request);
        }
    }
}

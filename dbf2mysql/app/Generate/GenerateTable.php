<?php

namespace App\Generate;

use App\services\PDOmap;
use Exception;

/**
 * Генерирование таблиц на стороне БД
 *  
 * @version 0.0.3
 */

final class GenerateTable
{
    /**
     * PDO соединение
     *
     * @var object
     */
    private $pdo;
    /**
     * Объект логирования SQL запросов
     *
     * @var object
     */
    private $PDOmap;
    /**
     * Конструктор класса принимает объект PDOmap
     *  -PDOmap - логирует запросы к бд
     */

    public function __construct()
    {
        $this->PDOmap = new PDOmap();
    }
    /**
     * Метод для таска nci
     *
     * @param  object $pdo - коннект к БД
     *
     * @return void
     */
    public function nci($pdo)
    {
        $this->pdo = $pdo;
        $query_drop = [
            "DROP TABLE IF EXISTS nci_test.inv_tmp_contracts;",
            "DROP TABLE IF EXISTS nci_test.inv_tmp_providers;",
            "DROP TABLE IF EXISTS nci_test.inv_tmp_country;",
            "DROP TABLE IF EXISTS nci_test.inv_tmp_units;",
            "DROP TABLE IF EXISTS nci_test.tmp_prof_level;",
            "DROP TABLE IF EXISTS nci_test.inv_tmp_contracts;",
            "DROP TABLE IF EXISTS nci_test.tmp_podr_all;",
            "DROP TABLE IF EXISTS nci_test.nci_tmp_podr_all;",
        ];
        $query = [
            "CREATE TABLE nci_test.inv_tmp_contracts(SELECT CONCAT(t1.id, t1.year) as id, t1.year, t1.num_dog as num_contr, t1.ddog as from_date, t1.str_kod, t1.kpost, t1.npost, t1.vid_dog, t1.svn_kod, t1.ddog as kurs_date, t1.summa, '' as date_last_deal, t1.kon_beg, t1.kon_end, t1.podr, t1.ispol, t1.d_opl, t1.d_reg
			FROM nci_test.nci_p160901 t1
			WHERE t1.pr_zakr = '0' AND ((t1.podr LIKE '01%') or (t1.podr LIKE '02%'))
			GROUP BY CONCAT(t1.id, t1.year) ORDER BY t1.kon_end);",

            "CREATE TABLE nci_test.inv_tmp_providers(SELECT kpost, npost, kod_str, inn, adres FROM nci_test.nci_r233 ORDER BY kpost ASC);",

            "CREATE TABLE nci_test.inv_tmp_country(SELECT CAST(str_kod as SIGNED) as id, str_k2 as code, str_name, str_name_a as e_str_name FROM nci_test.nci_pn161 ORDER BY str_kod ASC);",

            "CREATE TABLE nci_test.inv_tmp_units(SELECT eizm as unit_code, LCASE(naimeizm) as name, nm_ei as def FROM nci_test.nci_r241);",

            "CREATE TABLE nci_test.tmp_prof_level(SELECT *, kod_ur as `level`, CURDATE() as date_from, '2038-01-01' as date_to 
            FROM nci_test.nci_r254 WHERE SUBSTRING(gost,1,1) LIKE '2');",

            "CREATE TABLE nci_test.tmp_podr_all(SELECT tt.*, CURDATE() as date_from, '2038-01-01' as date_to FROM(
                (SELECT CONCAT(kod_pr,kod_ceh,'000000') code, 
                IF(LOCATE('ЦЕХ', nceh)>0,'1 Цех','1 Отдел') type_row, 
                kod_pr, kod_sl, IF (kod_pr='01','МЗХ','ЗБТ') knpr,
                CONCAT(kod_pr,kod_ceh) kod_ceh, nceh, knceh, 
                '' kod_admu, '' nadmu, '' knadmu, '' kod_master, '' kod_brig, '' nbrig, '' knbrig
                FROM nci_test.nci_r243ceh 
                WHERE kod_pr IN ('01', '02') AND priz_arh !=1 AND (data_do > CURDATE()) 
                    AND (data_ot <= '2038-01-01')) 
                UNION
                (SELECT CONCAT(t1.kod_pr,t1.kod_ceh,t1.kod_admu,'0000') code, 
                IF(LOCATE('УЧАСТОК', t1.nadmu)>0,'2 Участок','2 Бюро') type_row,
                t1.kod_pr, t2.kod_sl, IF (t1.kod_pr='01','МЗХ','ЗБТ') knpr,
                CONCAT(t1.kod_pr,t1.kod_ceh) kod_ceh, t2.nceh, t2.knceh, t1.kod_admu, 
                t1.nadmu, t1.knadmu, '' kod_master, CONCAT(IFNULL(t1.kod_admu,'00'), '0000') kod_brig,
                '' nbrig, '' knbrig

                FROM nci_test.nci_r243admu t1 
                INNER JOIN nci_test.nci_r243ceh t2 ON (t2.kod_ceh = t1.kod_ceh AND t2.kod_pr = t1.kod_pr 
                    AND t2.priz_arh !=1 AND (t2.data_do > CURDATE()) AND (t2.data_ot <= CURDATE())) 

                WHERE t1.kod_pr IN ('01', '02') AND t1.priz_arh !=1 AND (t1.data_do > CURDATE()) 
                    AND (t1.data_ot <= CURDATE()))

                UNION 
            (SELECT CONCAT(t1.kod_pr,t1.kod_ceh,t1.kod_admu,t3.kod_master,t3.kod_brig) code, 
                IF(LOCATE('БРИГАДА', t3.nbrig)>0,'3 Бригада','3 Группа') type_row,
                t1.kod_pr, t2.kod_sl, IF (t1.kod_pr='01','МЗХ','ЗБТ') knpr,
                CONCAT(t1.kod_pr,t1.kod_ceh) kod_ceh, t2.nceh, t2.knceh, t1.kod_admu, 
                t1.nadmu, t1.knadmu, t3.kod_master, 
                CONCAT(IFNULL(t1.kod_admu,'00'), IFNULL(t3.kod_master,'00'), IFNULL(t3.kod_brig,'00')) kod_brig,
                t3.nbrig, t3.knbrig
    
                FROM nci_test.nci_r243mbr t3
                INNER JOIN nci_test.nci_r243admu t1 ON (t3.kod_admu = t1.kod_admu AND t3.kod_ceh = t1.kod_ceh 
                    AND t3.kod_pr = t1.kod_pr AND t3.priz_arh !=1 AND (t3.data_do > CURDATE()) 
                    AND (t3.data_ot <= CURDATE())) 
                INNER JOIN nci_test.nci_r243ceh t2 ON (t2.kod_ceh = t1.kod_ceh AND t2.kod_pr = t1.kod_pr 
                    AND t2.priz_arh !=1 AND (t2.data_do > CURDATE()) AND (t2.data_ot <= CURDATE()))
                WHERE t1.kod_pr IN ('01', '02') AND t1.priz_arh !=1 AND (t1.data_do > CURDATE()) 
                    AND (t1.data_ot <= CURDATE()))
            )tt

            ORDER BY tt.code ASC);",

            "CREATE TABLE nci_test.nci_tmp_podr_all(SELECT t1.*, t2.knsl, t2.nsl,  
            CONCAT(t1.knpr, ' [', t1.kod_pr, ']') s_pr, 
            CONCAT(t2.knsl, ' [', t1.kod_sl, ']') s_sl,
            CONCAT(t1.knceh, ' [', SUBSTRING(t1.kod_ceh,3,2), ']') s_ceh,
            CONCAT(t1.knadmu, ' [', t1.kod_admu, ']') s_admu,
            CONCAT(t1.knbrig, ' [', SUBSTRING(t1.kod_brig,3,4), ']') s_brig      
        
        FROM nci_test.tmp_podr_all t1
        INNER JOIN nci_test.nci_r243sl t2 ON (t1.kod_pr = t2.kod_pr AND t1.kod_sl = t2.kod_sl)
        WHERE (t2.priz_arh= '0') AND (t2.data_do > CURDATE()) AND (t2.data_ot <= CURDATE()))",




        ];

        $this->generate($query, $query_drop);
    }
    /**
     * Метод для таска invoices
     *
     * @param  object $pdo - коннект к БД
     *
     * @return void
     */
    public function invoices($pdo)
    {
        $this->pdo = $pdo;
        $query_drop = [
            "DROP TABLE IF EXISTS invoices_test.inv_tmp_contracts;",
        ];
        $query = [
            "CREATE TABLE invoices_test.inv_tmp_contracts(Select * from nci_test.inv_tmp_contracts)",
        ];
        $this->generate($query, $query_drop);
    }
    /**
     * Метод для таска tb
     *
     * @param  object $pdo - коннект к бд 
     *
     * @return void
     */
    public function tb($pdo)
    {
        $this->pdo = $pdo;
        $query_drop = [
            "DROP TABLE IF EXISTS tb.m_tmp1;",
            "DROP TABLE IF EXISTS tb.m_tmp2;",
        ];
        $query = [
            //для этой таблицы зависимые таблицы имели префикс m_
            "CREATE TABLE tb.m_tmp1(SELECT t1.fm, t1.ima, t1.oth, t1.tn, t1.podr, t2.gr_nkat, t1.opd prof, t4.pnprof, t5.nceh, t5.knceh, t7.knsl, t1.dr, t1.sex, t1.dpr, t6.knadmu
            FROM nci_test.nci_osdalu  t1 
            JOIN nci_test.nci_r252 t2 ON t1.kat=t2.kateg
            LEFT JOIN m_r254 t4 ON t1.opd = t4.gost
            
            INNER JOIN nci_test.nci_r243ceh t5 ON (SUBSTRING(t1.podr, 1, 4) = CONCAT(t5.kod_pr, t5.kod_ceh) 
                AND (t5.data_do > curdate()) AND (t5.data_ot <= curdate() )
                AND t5.priz_arh= '0'
            )
            
            JOIN nci_test.nci_r243sl t7 ON (t5.kod_pr = t7.kod_pr AND t5.kod_sl = t7.kod_sl
                AND (t7.data_do > curdate()) AND (t7.data_ot <= curdate() )
                AND (t7.priz_arh= '0')
            )
            
            LEFT JOIN nci_test.nci_r243admu t6 ON (SUBSTRING(t1.podr,1,6)=CONCAT(t6.kod_pr, t6.kod_ceh, t6.kod_admu)
                AND (t6.data_do > curdate()) AND (t6.data_ot <= curdate())
                AND (t6.priz_arh= '0')
            )
            
            WHERE t1.tn not in (SELECT t6.tn
                FROM nci_test.nci_kotp t6
                where t6.kind = '70'
                AND (t6.beg < curdate()) 
                AND (t6.endo >= curdate()))
            ORDER BY BINARY t1.fm, t1.ima, t1.oth);",
        ];
        $this->generate($query, $query_drop);
    }

    /**
     * дерево подчинённости
     *
     * @param  PDO $pdo
     *
     * @return void
     */
    public function subordination($pdo)
    {
        $this->pdo = $pdo;

        $query_drop = [
            "DROP TABLE IF EXISTS subordination.staff;",
            "DROP TABLE IF EXISTS subordination.people;",
            "DROP TABLE IF EXISTS subordination.tree;",
        ];

        $query = [
            /**
             * @todo Поле fm  по нормальному нужно убрать,нормализация данных
             */
         
            "CREATE TABLE subordination.staff(SELECT if(t1.tn,t1.tn,null) as tn_people,t1.fm,t1.gr,t1.kod_pr,t1.podr,t1.kateg,t1.kprofpl,t2.kod_ur,t1.kolst,t1.cex,t1.uch,t1.master,t1.brig,
                 if(t1.tnvrrab,t1.tnvrrab,null) as tnvrrab,t2.nprof,t1.nom_pol 
                 from subordination.fschtat t1 
                 join subordination.r254 t2 on(t2.gost = t1.kprofpl)) order by kod_pr,podr,kod_ur DESC",
            "CREATE TABLE subordination.people(SELECT t1.tn,t1.fm,t1.ima,t1.oth,t1.podr,t1.sex from subordination.osdalu t1)",
            "CREATE TABLE subordination.tree(boss int(11) null,staff int(11) null)",
        ];
        $this->generate($query, $query_drop);
    }
    /**
     * ПДПЛ. Справочники
     *  @todo для 39 справочника таблица people берётся из другой схемы
     * @param  PDO $pdo
     *
     * @return void
     */
    public function directory($pdo)
    {
        $this->pdo = $pdo;

        $query_drop = [
            "DROP TABLE IF EXISTS pdpl.tree;",
            "DROP TABLE IF EXISTS pdpl.staff;",
			"DROP TABLE IF EXISTS pdpl.people",
			 "DROP TABLE IF EXISTS pdpl.digest6;",
			 "DROP TABLE IF EXISTS pdpl.digest7;",
			 "DROP TABLE IF EXISTS pdpl.digest8;",
             "DROP TABLE IF EXISTS pdpl.digest11;",
			"DROP TABLE IF EXISTS pdpl.digest12;",
			"DROP TABLE IF EXISTS pdpl.digest17;",
             "DROP TABLE IF EXISTS pdpl.digest19;",
             "DROP TABLE IF EXISTS pdpl.digest26;",
            "DROP TABLE IF EXISTS pdpl.digest29;",
             "DROP TABLE IF EXISTS pdpl.digest30;",
            // // "DROP TABLE IF EXISTS pdpl.digest31;",
            // // "DROP TABLE IF EXISTS pdpl.digest34;",
             "DROP TABLE IF EXISTS pdpl.digest36;",
			 "DROP TABLE IF EXISTS pdpl.digest37;",
			 "DROP TABLE IF EXISTS pdpl.digest46;",
			 "DROP TABLE IF EXISTS pdpl.digest68;",
			 "DROP TABLE IF EXISTS pdpl.digest69;",
             "DROP TABLE IF EXISTS pdpl.digest72;",
             "DROP TABLE IF EXISTS pdpl.digest28;",
             "DROP TABLE IF EXISTS pdpl.digest47;",
			"DROP TABLE IF EXISTS pdpl.digest50;",
            "DROP TABLE IF EXISTS pdpl.digest51;",
			"DROP TABLE IF EXISTS pdpl.digest52;",
			"DROP TABLE IF EXISTS pdpl.digest81;",
            "DROP TABLE IF EXISTS pdpl.digest_education;"
        ];

        $query = [
            "CREATE TABLE pdpl.tree SELECT * FROM subordination.tree",
            "CREATE TABLE pdpl.staff SELECT * FROM subordination.staff",
            "CREATE TABLE pdpl.people SELECT * FROM subordination.people",
			"CREATE TABLE pdpl.digest6 SELECT 
    IF(t1.kod_sl NOT LIKE '01',
        '1 Отдел',
        '1 Цех') AS structure,
    '1' AS struct_num,
    t1.podr,
    t1.kod_pr,
    UPPER(t2.knsl) AS kod_sl,
    CONCAT(t1.kod_pr, SUBSTRING(t1.podr, 1, 2)) AS kod_otd,
    UPPER(t1.knpodr) AS ncex,
    '' AS nuch,
    '' AS nbrig,
    '000000' AS kod,
    '' AS uch,
    '' AS brig
FROM
    pdpl.r243 t1
        LEFT JOIN
    pdpl.r243sl t2 ON (t1.kod_pr = t2.kod_pr
        AND t1.kod_sl = t2.kod_sl
        AND t2.priz_arh = 0)
WHERE
    (t1.kod_sl NOT LIKE '91'
        AND SUBSTRING(t1.podr, 3, 8) LIKE '00000000'
        AND SUBSTRING(t1.podr, 1, 2) NOT LIKE '00'
        AND t1.kod_pr IN ('01' , '02', '03')) 
UNION SELECT 
    IF(((t1.kod_sl LIKE '01')
            AND (SUBSTRING(t1.podr, 3, 2) NOT LIKE '76')),
        '2 Участок',
        '2 Бюро') AS structure,
    '2' AS struct_num,
    t1.podr,
    t1.kod_pr,
    UPPER(t2.knsl) AS kod_sl,
    CONCAT(t1.kod_pr, SUBSTRING(t1.podr, 1, 2)) AS kod_otd,
    SUBSTRING_INDEX(UPPER(t1.knpodr), ' ', 1) AS ncex,
    UPPER(t1.knpodr) AS nuch,
    '' AS nbrig,
    SUBSTRING(t1.podr, 3, 6) AS kod,
    UPPER(t1.npodr) AS uch,
    '' AS brig
FROM
    pdpl.r243 t1
        LEFT JOIN
    pdpl.r243sl t2 ON (t1.kod_pr = t2.kod_pr
        AND t1.kod_sl = t2.kod_sl
        AND t2.priz_arh = 0)
WHERE
    (t1.kod_sl NOT LIKE '91'
        AND SUBSTRING(t1.podr, 5, 6) LIKE '000000'
        AND SUBSTRING(t1.podr, 1, 4) NOT LIKE '%00'
        AND t1.kod_pr IN ('01' , '02', '03')) 
UNION SELECT 
    '3 Группа' AS structure,
    '3' AS struct_num,
    t1.podr,
    t1.kod_pr,
    UPPER(t2.knsl) AS kod_sl,
    CONCAT(t1.kod_pr, SUBSTRING(t1.podr, 1, 2)) AS kod_otd,
    SUBSTRING_INDEX(UPPER(t1.knpodr), ' ', 1) AS ncex,
    SUBSTRING_INDEX(UPPER(t1.knpodr), ' ', 2) AS nuch,
    UPPER(t1.knpodr) AS nbrig,
    CONCAT(SUBSTRING(t1.podr, 3, 2),
            SUBSTRING(t1.podr, 7, 4)) AS kod,
    UPPER(t3.npodr) AS uch,
    UPPER(t1.npodr) AS brig
FROM
    pdpl.r243 t1
        LEFT JOIN
    pdpl.r243sl t2 ON (t1.kod_pr = t2.kod_pr
        AND t1.kod_sl = t2.kod_sl
        AND t2.priz_arh = 0)
        LEFT JOIN
    pdpl.r243 t3 ON (SUBSTRING(t1.podr, 1, 8) = SUBSTRING(t3.podr, 1, 8)
        AND t3.kod_sl NOT LIKE '91'
        AND SUBSTRING(t3.podr, 5, 6) LIKE '000000'
        AND SUBSTRING(t3.podr, 1, 4) NOT LIKE '0000'
        AND t3.kod_pr = t1.kod_pr
        AND t3.kod_pr IN ('01' , '02', '03'))
WHERE
    (t1.kod_sl NOT LIKE '91'
        AND SUBSTRING(t1.podr, 5, 4) LIKE '0000'
        AND SUBSTRING(t1.podr, 1, 4) NOT LIKE '00'
        AND SUBSTRING(t1.podr, 9, 2) NOT LIKE '00'
        AND t1.kod_pr IN ('01' , '02', '03'))
ORDER BY structure;",
			"CREATE TABLE pdpl.digest7 SELECT DISTINCT
				'4 Профессия' as structure,
				'4' as struct_num,
				r1.gost AS kprofpl,

				UPPER(r1.pnprof) as profname,
				f1.kod_pr,
				UPPER(r3.knsl) as sl,
				CONCAT(f1.kod_pr,r2.kod_ceh) as kod,
				UPPER(r2.knceh) as cexname,
                r4.podr,
				UPPER(r4.knpodr) as npodr
			FROM
				pdpl.r254 r1
					JOIN
				pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
                JOIN
				pdpl.r243 r4 ON (CONCAT(SUBSTRING(r4.podr, 1, 4),
            SUBSTRING(r4.podr, 7, 4)) = f1.podr and SUBSTRING(r4.podr,7,2) = '00' and r4.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
					AND r2.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
					AND r3.kod_pr = r2.kod_pr
					AND r3.priz_arh = 0)
			WHERE
				SUBSTRING(r1.gost, 1, 1) = 2",
			"CREATE TABLE pdpl.digest8 SELECT 
	'2 Бюро/Участок' AS structure,
    f1.kod_pr,
    UPPER(r3.knsl) AS sl,
    CONCAT(f1.kod_pr, r2.kod_ceh) AS kod,
    UPPER(r2.knceh) AS cexname,
    SUBSTRING(r4.podr, 3, 2) as podr,
    UPPER(r4.knpodr) AS npodr,
    '00' AS kodbrig,
    '' AS brig,
    r1.gost AS kprofpl,
    UPPER(r1.pnprof) AS profname
FROM
    pdpl.r254 r1
        JOIN
    pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
        JOIN
    pdpl.r243 r4 ON (CONCAT(SUBSTRING(r4.podr, 1, 4),
            SUBSTRING(r4.podr, 7, 4)) = f1.podr
        AND SUBSTRING(r4.podr, 5, 6) = '000000'
        AND r4.kod_pr = f1.kod_pr)
        JOIN
    pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
        AND r2.kod_pr = f1.kod_pr)
        JOIN
    pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
        AND r3.kod_pr = r2.kod_pr
        AND r3.priz_arh = 0)
WHERE
    SUBSTRING(r1.gost, 1, 1) = 2
GROUP BY kod , podr , kprofpl
UNION
SELECT 
		'3 Группа/Бригада' AS structure,
				f1.kod_pr,
				UPPER(r3.knsl) as sl,
				CONCAT(f1.kod_pr,r2.kod_ceh) as kod,
				UPPER(r2.knceh) as cexname,
				SUBSTRING(r4.podr, 3, 2) as podr,
				UPPER(r4.knpodr) as npodr,
                SUBSTRING(r4.podr, 9, 2) as kodbrig, 
                UPPER(r4.npodr) as brig,
                r1.gost AS kprofpl,
				UPPER(r1.pnprof) as profname
			FROM
				pdpl.r254 r1
					JOIN
				pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
			 JOIN
  				pdpl.r243 r4 ON (CONCAT(SUBSTRING(r4.podr, 1, 4),
            SUBSTRING(r4.podr, 7, 4)) = f1.podr and SUBSTRING(r4.podr, 5, 4) = '0000' and SUBSTRING(r4.podr,9,2) <> '00' and r4.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
					AND r2.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
					AND r3.kod_pr = r2.kod_pr
					AND r3.priz_arh = 0)
			WHERE
				SUBSTRING(r1.gost, 1, 1) = 2
                group by kod,podr, kodbrig, kprofpl",
             "CREATE TABLE pdpl.digest11 SELECT tariff.kod,tariff.naim FROM pdpl.r254kkk tariff;",
			"CREATE TABLE pdpl.digest12 SELECT DISTINCT 
				r1.gost AS kprofpl,
				r1.pnprof,
				f1.kod_pr,
				r3.knsl,
				r2.kod_ceh AS cex,
				r2.knceh
			FROM
				pdpl.r254 r1
					JOIN
				pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
					JOIN
				pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
					AND r2.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
					AND r3.kod_pr = r2.kod_pr
					AND r3.priz_arh = 0)
			WHERE
				SUBSTRING(r1.gost, 1, 1) = 2",
				"CREATE TABLE pdpl.digest17 SELECT 
				f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.master,f0_.gr,f0_.tn,
                 IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
                 f0_.opd,f0_.kolst,t1.odpr as odprps,f0_.podr,f0_.kateg,
                 f0_.kprofpl,IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol,r1_.pnprof,r1_.gost,r1_.kod_ur,
                 r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,t10.tarif as n_cts, t10.chts1,
                 IF( ((YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)))>0,(YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)),0) AS data_r,
                 0 as missing, 
                 f0_.tk_pl as tk_f,  ROUND(f0_.tk_pl * t10.chts1*168, 3)  as to1, f0_.k1, f0_.k2, f0_.k3, f0_.k5,
                         round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) as okkl6, f0_.proc_77f as nad6,
                          round(( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * f0_.proc_77f / 100, 2 ) as nad6_sum,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * 20 / 100, 2) as prem,
                         d20.value as stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * d20.value /100, 2) as totla_stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * f0_.proc_77f / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * d20.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * 20 / 100) as total,
                         round(f0_.kolst * ( round(round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * f0_.proc_77f / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * d20.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*168, 3) +  ROUND(f0_.tk_pl * t10.chts1*168, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + 50) / 100) * 20 / 100))) as total_s
                 FROM
                 pdpl.fschtat f0_
                     INNER JOIN pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                     INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
                    INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r2_.priz_arh = 0 and r3_.priz_arh = 0))
                     INNER JOIN pdpl.knap t1 on(f0_.tn = t1.tn and substr(t1.podr,3,8)= f0_.podr and t1.dol = f0_.kprofpl)
					 LEFT JOIN pdpl.people p1_ ON ((p1_.tn = f0_.tn))
                     left join pdpl.spr_tarif t9 on f0_.kprofpl=t9.kprofpl and f0_.kod_pr=t9.kod_pr and f0_.cex=t9.cex and f0_.kateg=t9.kateg and t9.brig = f0_.brig and f0_.tn=t9.tn and f0_.master=t9.master 
							left join pdpl.r244 t10 on (t10.tarif=t9.tarif and t9.tarif is not null)
                            INNER JOIN (select k1.tn, MAX(year(From_DAYS(datediff(now(),k1.odpr)))) as odpr from pdpl.knap k1 group by k1.tn) kn1 on kn1.tn = f0_.tn
							left join pdpl.digest20 d20 on (d20.year_from<=kn1.odpr and d20.year_to > kn1.odpr and d20.year_to is not null 
							or d20.year_to is null and d20.year_from<=kn1.odpr) 
                 WHERE
                     f0_.tn != '' OR f0_.tnvrrab != '' group by t1.tn;",
             "CREATE TABLE pdpl.digest19 SELECT DISTINCT
                 r1_.kprof,
                 r1_.nprof,
                 f0_.kod_pr,
                 f0_.cex,
                 f0_.uch,
                 r1_.kod_ur
                 FROM pdpl.r254 r1_
                 JOIN pdpl.fschtat f0_ ON (r1_.gost = f0_.kprofpl);", //
				 "CREATE TABLE pdpl.digest69 SELECT
                 distinct f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.gr,f0_.tn,
                 IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
                 f0_.opd,f0_.kolst,f0_.podr,
                 f0_.kprofpl,IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol,r1_.pnprof,r1_.gost,r1_.kod_ur,
                 r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,t10.tarif as n_cts, t10.chts1,
				 IF((k1_.kind=70 or k1_.kind=72), 
                CONCAT(f0_.tnvrrab,' (',UCASE(LEFT(p1.fm, 1)),
                     SUBSTRING(p1.fm, 2),
                     ' ',
                     UPPER(SUBSTR(p1.ima, 1, 1)),
                     '.',
                     UPPER(SUBSTR(p1.oth, 1, 1)),
                     ')')
             , '') as ptworker6,f0_.tk_pl as tk_f,  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3)  as to1, f0_.k1, f0_.k2, f0_.k3, f0_.k5, dp4.value as k4, dp3.value as days,

                         round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) as okkl6, dp.value as nad6,
                          round(( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * dp.value / 100, 2 ) as nad6_sum,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * 20 / 100, 2) as prem,
                          dp2.value as stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) *  dp2.value /100, 2) as totla_stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * dp.value / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) *  dp2.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * 20 / 100) as total,
                         round(f0_.kolst * ( round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * dp.value / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) *  dp2.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value ) / 100) * 20 / 100))) as total_s
                 FROM
                     pdpl.fschtat f0_
                         INNER JOIN
                     pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                         INNER JOIN
                     pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex
                         AND r2_.kod_pr = f0_.kod_pr
                         AND r2_.priz_arh = 0))
                         INNER JOIN
                     pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl
                         AND r3_.kod_pr = r2_.kod_pr
                         AND r3_.priz_arh = 0))
                         LEFT JOIN pdpl.people p1_ ON (p1_.tn = f0_.tn)
                         LEFT JOIN pdpl.people p1 ON (p1.tn = f0_.tnvrrab)
                         LEFT JOIN pdpl.digest0 dp ON (dp.parameter = 'default_percent77')
                         LEFT JOIN pdpl.digest0 dp2 ON (dp2.parameter = 'default_proc_s')
                         LEFT JOIN pdpl.digest0 dp3 ON (dp3.parameter = 'hours_per_month')
                         LEFT JOIN pdpl.digest0 dp4 ON (dp4.parameter = 'percent_for_contract')
                            left join pdpl.spr_tarif t9 on f0_.kprofpl=t9.kprofpl and f0_.kod_pr=t9.kod_pr and f0_.cex=t9.cex and f0_.kateg=t9.kateg and t9.brig = f0_.brig and f0_.tn=t9.tn and f0_.master=t9.master 
							left join pdpl.r244 t10 on (t10.tarif=t9.tarif and t9.tarif is not null)
                         left JOIN (select k1.tn, MAX(year(From_DAYS(datediff(now(),k1.odpr)))) as odpr from pdpl.knap k1 group by k1.tn) kn1 on kn1.tn = f0_.tn
							left join (select * from pdpl.digest20) d20 on (d20.year_from<=kn1.odpr and d20.year_to > kn1.odpr and d20.year_to is not null 
							or d20.year_to is null and d20.year_from<=kn1.odpr)
						left JOIN  pdpl.kotp k1_ ON(f0_.tn = k1_.tn)",
			"CREATE TABLE pdpl.digest68 SELECT 
				 DISTINCT f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.gr,f0_.tn,
                 IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
                 f0_.opd,f0_.kolst,f0_.podr,f0_.kateg,
                 f0_.kprofpl,IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol,r1_.pnprof,r1_.gost,r1_.kod_ur,
                 r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,t10.tarif as n_cts, t10.chts1,
                 IF( ((YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)))>0,(YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)),0) as data_r,
                 CONCAT(f0_.tnvrrab,' (',UCASE(LEFT(p1.fm, 1)),
                         SUBSTRING(p1.fm, 2),
                         ' ',
                         UPPER(SUBSTR(p1.ima, 1, 1)),
                         '.',
                         UPPER(SUBSTR(p1.oth, 1, 1)),
                         ')') as ptworker6, f0_.tk_pl as tk_f,  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3)  as to1, f0_.k1, f0_.k2, f0_.k3, f0_.k5, dp4.value as k4, dp3.value as days,
                         round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) as okkl6, f0_.proc_77f as nad6,
                          round(( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * f0_.proc_77f / 100, 2 ) as nad6_sum,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * 20 / 100, 2) as prem,
                         d20.value as stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * d20.value /100, 2) as totla_stazh,
                         round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * f0_.proc_77f / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * d20.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * 20 / 100) as total,
                         round(f0_.kolst * ( round(round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * f0_.proc_77f / 100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * d20.value /100 
                         + round( ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) +  ROUND(f0_.tk_pl * t10.chts1*dp3.value, 3) * (f0_.k1 + f0_.k2 + f0_.k3 + f0_.k5 + dp4.value) / 100) * 20 / 100))) as total_s
                 FROM
                     pdpl.fschtat f0_
                         INNER JOIN
                     pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                         INNER JOIN
                     pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex
                         AND r2_.kod_pr = f0_.kod_pr
                         AND r2_.priz_arh = 0))
                         INNER JOIN
                     pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl
                         AND r3_.kod_pr = r2_.kod_pr
                         AND r3_.priz_arh = 0))
                         INNER JOIN pdpl.knap t1 on(f0_.tn = t1.tn and substr(t1.podr,3,8)= f0_.podr and t1.dol = f0_.kprofpl)
                         LEFT JOIN pdpl.people p1_ ON (p1_.tn = f0_.tn)
						LEFT JOIN pdpl.digest0 dp3 ON (dp3.parameter = 'hours_per_month')
                         LEFT JOIN pdpl.digest0 dp4 ON (dp4.parameter = 'percent_for_contract')
                         LEFT JOIN pdpl.people p1 ON (p1.tn = f0_.tnvrrab)
                                            left join pdpl.spr_tarif t9 on f0_.kprofpl=t9.kprofpl and f0_.kod_pr=t9.kod_pr and f0_.cex=t9.cex and f0_.kateg=t9.kateg and t9.brig = f0_.brig and f0_.tn=t9.tn and f0_.master=t9.master 
							left join pdpl.r244 t10 on (t10.tarif=t9.tarif and t9.tarif is not null)
                            INNER JOIN (select k1.tn, MAX(year(From_DAYS(datediff(now(),k1.odpr)))) as odpr from pdpl.knap k1 group by k1.tn) kn1 on kn1.tn = f0_.tn
							left join pdpl.digest20 d20 on (d20.year_from<=kn1.odpr and d20.year_to > kn1.odpr and d20.year_to is not null 
							or d20.year_to is null and d20.year_from<=kn1.odpr) 
                            where (f0_.tn != '' and f0_.tnvrrab='')  or (f0_.tn!='' and f0_.tnvrrab!='' and f0_.tn != f0_.tnvrrab)
                         GROUP BY f0_.tn;", //
             "CREATE TABLE pdpl.digest72 SELECT 
                distinct f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.gr,f0_.tn,
                IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
                f0_.opd,f0_.kolst,f0_.podr,
                f0_.kprofpl,IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol,r1_.pnprof,r1_.gost,r1_.kod_ur,
                r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,
                IF((k1_.kind=70 or k1_.kind=72), 
                CONCAT(f0_.tnvrrab,' (',UCASE(LEFT(p1.fm, 1)),
                     SUBSTRING(p1.fm, 2),
                     ' ',
                     UPPER(SUBSTR(p1.ima, 1, 1)),
                     '.',
                     UPPER(SUBSTR(p1.oth, 1, 1)),
                     ')')
             , '') as ptworker
             FROM
                 pdpl.fschtat f0_
                     INNER JOIN
                 pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                     INNER JOIN
                 pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex
                     AND r2_.kod_pr = f0_.kod_pr
                     AND r2_.priz_arh = 0))
                     INNER JOIN
                 pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl
                     AND r3_.kod_pr = r2_.kod_pr
                     AND r3_.priz_arh = 0))
                     LEFT JOIN pdpl.people p1_ ON (p1_.tn = f0_.tn)
                     LEFT JOIN pdpl.people p1 ON (p1.tn = f0_.tnvrrab)
                     LEFT JOIN
                 pdpl.kotp k1_ ON(f0_.tn = k1_.tn)  ", //
             "CREATE TABLE pdpl.digest29 SELECT 
                 f0_.kod_pr,f0_.cex,f0_.podr,f0_.uch,f0_.brig,f0_.gr,IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol,
                 f0_.tn,IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,f0_.opd,f0_.kolst,f0_.kateg,
			 	  IF( ((YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)))>0,(YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)),0) as data_r,
                         r1_.pnprof,
                         r1_.gost,r1_.kod_ur,r2_.knceh,r2_.kod_sl,r3_.knsl,
                         r3_.nsl,
                         0 as missing
                     FROM
                         pdpl.fschtat f0_
                         INNER JOIN pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                         INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
                         INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r2_.priz_arh = 0 and r3_.priz_arh = 0))
                         INNER JOIN pdpl.knap t1 on(f0_.tn = t1.tn and substr(t1.podr,3,8)= f0_.podr and t1.dol = f0_.kprofpl)
						LEFT JOIN pdpl.people p1_ ON ((p1_.tn = f0_.tn))
                     WHERE (f0_.tn != '' and f0_.tnvrrab='') or (f0_.tn!='' and f0_.tnvrrab!='' and f0_.tn != f0_.tnvrrab) group by t1.tn;",
             "CREATE TABLE pdpl.digest26 SELECT 
                 f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.master,f0_.gr,f0_.tn,
                 IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
				 f0_.opd,f0_.kolst,t1.odpr as odprps,f0_.kateg,r1_.pnprof,r1_.gost,r1_.kod_ur,
                 r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,
                 IF( ((YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)))>0,(YEAR(now()) - YEAR(t1.odpr)) * 12 + (MONTH(now()) - MONTH(t1.odpr)),0) AS data_r,
                 0 as missing
                 FROM
                 pdpl.fschtat f0_
                     INNER JOIN pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                     INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
                    INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r2_.priz_arh = 0 and r3_.priz_arh = 0))
                     INNER JOIN pdpl.knap t1 on(f0_.tn = t1.tn and substr(t1.podr,3,8)= f0_.podr and t1.dol = f0_.kprofpl)
					 LEFT JOIN pdpl.people p1_ ON ((p1_.tn = f0_.tn))
                 WHERE
                     f0_.tn != '' OR f0_.tnvrrab != '' group by t1.tn;",
             "CREATE TABLE pdpl.digest36 SELECT
                 f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.gr,f0_.tn,
                 IF(p1_.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2)))) AS fm,
                 f0_.opd,r1_.pnprof,r1_.gost,
                 r1_.kod_ur,r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl
                 FROM
                     pdpl.fschtat f0_
                         INNER JOIN
                     pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                         INNER JOIN
                     pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex
                         AND r2_.kod_pr = f0_.kod_pr and r2_.priz_arh = 0))
                         INNER JOIN
                     pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr and r2_.priz_arh = 0 and r3_.priz_arh = 0)) 
                     LEFT JOIN pdpl.people p1_ ON ((p1_.tn = f0_.tn))
                     WHERE r1_.kod_ur >= 30", //

			"CREATE TABLE pdpl.digest37 SELECT
                t1.kod_pr,			
                UPPER(t2.knsl) AS kod_sl,
                CONCAT(t1.kod_pr, SUBSTRING(t1.podr, 1, 2)) AS kod_otd,
                UPPER(t1.knpodr) AS ncex
            FROM
                pdpl.r243 t1
            LEFT JOIN
                pdpl.r243sl t2 ON (t1.kod_pr = t2.kod_pr
                AND t1.kod_sl = t2.kod_sl
                AND t2.priz_arh = 0)
            LEFT JOIN
                pdpl.fschtat f2 ON (t1.kod_pr = f2.kod_pr and f2.cex =  SUBSTRING(t1.podr, 1, 2) ) 
                JOIN pdpl.r254 r1 ON (r1.gost = f2.kprofpl)
            WHERE
                (t1.kod_sl NOT LIKE '91'
                AND SUBSTRING(t1.podr, 3, 8) LIKE '00000000'
                AND SUBSTRING(t1.podr, 1, 2) NOT LIKE '00'
                AND t1.kod_pr IN ('01' , '02', '03')) AND r1.kod_ur < 50 group by kod_otd HAVING count(f2.tn) > 0",
			"CREATE TABLE pdpl.digest30 SELECT table1.proff, IF(t2.proff IS NULL, table1.pnprof, t2.pnprof) as nprof, table1.kod_ur, table1.id as gost, do1.code_group AS base_kategory
				FROM (SELECT t1.gost AS id, t1.proff, t1.pnprof, t1.kod_ur
                FROM
                    pdpl.r254 t1
                    where SUBSTRING(t1.proff, 1, 1) = 2
					group by t1.proff) as table1
                    LEFT JOIN pdpl.directory_one as do1 ON((substr(table1.id,1,1) = do1.first and substr(table1.id,6,1) = do1.sixth and do1.position_level = 1 and table1.kod_ur = 50) 
															xor 
															(substr(table1.id,1,1) = do1.first and substr(table1.id,6,1) = do1.sixth and do1.position_level = 0 and table1.kod_ur < 50))
                    LEFT JOIN (SELECT t1.gost as id, t1.proff, t1.pnprof
                FROM
                    pdpl.r254 t1
                    where SUBSTRING(t1.proff, 1, 1) = 2
					group by t1.proff) as t2 ON( table1.proff=t2.proff and SUBSTRING(t2.id, 7, 10) LIKE '0000')
                    group by table1.proff",/* без group by table1.proff */	
             "CREATE TABLE pdpl.digest28 SELECT v.kateg,v.naim,v.gr_nkat FROM pdpl.r252 v", //
			 "CREATE TABLE pdpl.digest46 SELECT increase.kod,increase.naim FROM pdpl.r254kpd increase;",
			 "CREATE TABLE pdpl.digest47 SELECT DISTINCT f0_.kod_pr,r3_.knsl, f0_.cex, r2_.knceh, r1_.gost, r1_.pnprof 
			 	FROM 
			 		pdpl.r254 r1_ 
			 			JOIN 
			 		pdpl.fschtat f0_ ON ((f0_.kprofpl = r1_.gost))   
			 			JOIN 
			 		pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0)) 
			 			JOIN 
			 		pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r3_.priz_arh = 0))
			 	WHERE substring(r1_.gost,1,1) = 2
			 	GROUP BY r2_.knceh, r1_.gost",
            "CREATE TABLE pdpl.digest51 SELECT DISTINCT 
				r1.gost AS kprofpl,
				r1.nprof,
				r1.pnprof,
				f1.kod_pr,
				r3.knsl,
				r2.kod_ceh AS cex,
				r2.knceh
			FROM
				pdpl.r254 r1
					JOIN
				pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
					JOIN
				pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
					AND r2.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
					AND r3.kod_pr = r2.kod_pr
					AND r3.priz_arh = 0)
			WHERE
				SUBSTRING(r1.gost, 1, 1) = 2", //group by gost,kod_pr", //

			"CREATE TABLE pdpl.digest50 (id BIGINT(12) NOT NULL, base_code VARCHAR(5) NOT NULL UNIQUE, name VARCHAR(255) NOT NULL, PRIMARY KEY (id)) select table1.id, table1.proff as base_code,  IF(t2.proff IS NULL, table1.pnprof, t2.pnprof) as name from (SELECT t1.gost AS id, t1.proff, t1.pnprof
                FROM
                    pdpl.r254 t1
                    where SUBSTRING(t1.proff, 1, 1) = 2
					group by t1.proff) as table1 
                    Left JOIN (SELECT t1.gost as id, t1.proff, t1.pnprof
                FROM
                    pdpl.r254 t1
                    where SUBSTRING(t1.proff, 1, 1) = 2
					group by t1.proff) as t2 ON( table1.proff=t2.proff and SUBSTRING(t2.id, 7, 10) LIKE '0000')",
			"CREATE TABLE pdpl.digest52 (INDEX(cex) , INDEX(kod_pr), INDEX(kprofpl)) SELECT f0_.kod_pr,
				r3_.knsl,
				f0_.cex,
				r2_.knceh,
				f0_.uch,
				f0_.brig,
				f0_.tn,
			CONCAT_WS(' ', CONCAT(upper(left(p1_.fm,1)),substring(p1_.fm, 2)) , CONCAT(upper(left(p1_.ima,1)),substring(p1_.ima, 2)), CONCAT(upper(left(p1_.oth,1)),substring(p1_.oth, 2))) AS fio,
				f0_.kprofpl
			FROM
				pdpl.fschtat f0_
			INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
			INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r3_.priz_arh = 0))
			INNER JOIN pdpl.people p1_ ON ((p1_.tn = f0_.tn))
			WHERE
				SUBSTRING(f0_.kprofpl, 1, 1) = 2",
			"CREATE TABLE pdpl.digest_education (INDEX(tn)) SELECT DISTINCT ed1.tn,
				ed1.dok AS diploma_date,
				s1.name AS type_of_oducation,
				uz1.name_p AS university,
				uz1.gorod_u,
				s2.name AS speciality,
				s3.name AS qualification FROM
				pdpl.education ed1
					LEFT JOIN
				pdpl.spr s1 ON ((ed1.obr = s1.kod AND s1.type = 51))
					LEFT JOIN
				pdpl.spr s2 ON ((ed1.spec = s2.kod AND s2.type = 2))
					LEFT JOIN
				pdpl.spr s3 ON ((ed1.kfd = s3.kod AND s3.type = 3))
					LEFT JOIN
				pdpl.uch_z uz1 ON ((ed1.u_z = uz1.kod))",
				"CREATE TABLE pdpl.digest81 SELECT 
	'2 Бюро/Участок' AS structure,
    f1.kod_pr,
    UPPER(r3.knsl) AS sl,
    CONCAT(f1.kod_pr, r2.kod_ceh) AS kod,
    UPPER(r2.knceh) AS cexname,
    SUBSTRING(r4.podr, 3, 2) as podr,
    UPPER(r4.knpodr) AS npodr,
    '00' AS kodbrig,
    '' AS brig,
    r1.gost AS kprofpl,
    UPPER(r1.pnprof) AS profname,
    SUM(f1.kolst) AS ixbet,
    SUM(CASE
        WHEN
            (((f1.tn <> '') AND (f1.tnvrrab = ''))
                OR ((f1.tn = '') AND (f1.tnvrrab <> ''))
                OR ((f1.tn <> '') AND (f1.tnvrrab <> '')))
        THEN
            f1.kolst
        ELSE 0
    END) AS employee
FROM
    pdpl.r254 r1
        JOIN
    pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
        JOIN
    pdpl.r243 r4 ON (CONCAT(SUBSTRING(r4.podr, 1, 4),
            SUBSTRING(r4.podr, 7, 4)) = f1.podr
        AND SUBSTRING(r4.podr, 5, 6) = '000000'
        AND r4.kod_pr = f1.kod_pr)
        JOIN
    pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
        AND r2.kod_pr = f1.kod_pr)
        JOIN
    pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
        AND r3.kod_pr = r2.kod_pr
        AND r3.priz_arh = 0)
WHERE
    SUBSTRING(r1.gost, 1, 1) = 2
GROUP BY kod , podr , kprofpl
UNION
SELECT 
		'3 Группа/Бригада' AS structure,
				f1.kod_pr,
				UPPER(r3.knsl) as sl,
				CONCAT(f1.kod_pr,r2.kod_ceh) as kod,
				UPPER(r2.knceh) as cexname,
				SUBSTRING(r4.podr, 3, 2) as podr,
				UPPER(r4.knpodr) as npodr,
                SUBSTRING(r4.podr, 9, 2) as kodbrig, 
                UPPER(r4.npodr) as brig,
                r1.gost AS kprofpl,
				UPPER(r1.pnprof) as profname,
                SUM(f1.kolst) as ixbet,
                SUM(CASE WHEN (((f1.tn <> '') and (f1.tnvrrab = ''))
							or ((f1.tn = '') and (f1.tnvrrab <> ''))
                            or ((f1.tn <> '') and (f1.tnvrrab <> '')))
                            THEN 
                            1 
                            ELSE 
                            0 
                            END) as employee
			FROM
				pdpl.r254 r1
					JOIN
				pdpl.fschtat f1 ON (r1.gost = f1.kprofpl)
			 JOIN
  				pdpl.r243 r4 ON (CONCAT(SUBSTRING(r4.podr, 1, 4),
            SUBSTRING(r4.podr, 7, 4)) = f1.podr and SUBSTRING(r4.podr, 5, 4) = '0000' and SUBSTRING(r4.podr,9,2) <> '00' and r4.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243ceh r2 ON (r2.kod_ceh = f1.cex AND r2.priz_arh = 0
					AND r2.kod_pr = f1.kod_pr)
					JOIN
				pdpl.r243sl r3 ON (r3.kod_sl = r2.kod_sl
					AND r3.kod_pr = r2.kod_pr
					AND r3.priz_arh = 0)
			WHERE
				SUBSTRING(r1.gost, 1, 1) = 2
                group by kod,podr, kodbrig, kprofpl",
        ];
        $this->generate($query, $query_drop);
    }

    /**
     * Добавление данных в справочник Г29 и Г26
     * 
     * Добавляются те ШЕ у которых есть tnvrrab
     *
     * @param PDO $pdo - коннект к БД
     * 
     * @return void
     */
    public function insertData($pdo)
    {
        $this->pdo = $pdo;
        /// строка запроса
        $array = [
            "INSERT INTO pdpl.digest29(kod_pr,cex,uch,brig,gr,tn,fm,opd,kolst,kateg,data_r,pnprof,gost,kod_ur,knceh,kod_sl,knsl,nsl,nom_pol) SELECT 
                   f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.gr,t3.tn,
                   IF(t3.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(t3.fm,1)),substring(t3.fm, 2)) , CONCAT(upper(left(t3.ima,1)),substring(t3.ima, 2)), CONCAT(upper(left(t3.oth,1)),substring(t3.oth, 2)))) AS fm, 
                   f0_.opd,f0_.kolst,f0_.kateg,
            	    CASE
            			WHEN (year(From_DAYS(datediff(now(),t2.odpr))) > 0) THEN (year(From_DAYS(datediff(now(),t2.odpr)))*12) + month(From_DAYS(datediff(now(),t2.odpr))) 
                        else round(datediff(now(),t2.odpr)/30) 
            		END as data_r,
                        r1_.pnprof,
                        r1_.gost,r1_.kod_ur,r2_.knceh,r2_.kod_sl,r3_.knsl,
                        r3_.nsl,
						IF(f0_.nom_pol IS NULL, '', f0_.nom_pol) as nom_pol
                    FROM
                        pdpl.fschtat f0_
                        INNER JOIN pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                        INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
                        INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r2_.priz_arh = 0 and r3_.priz_arh = 0))
            			INNER JOIN pdpl.knap t2 on(f0_.tnvrrab = t2.tn and substr(t2.podr,3,8)= f0_.podr and t2.dol = f0_.kprofpl)
                        INNER JOIN pdpl.people t3 on(t3.tn = f0_.tnvrrab and f0_.tnvrrab !='')
                    WHERE (f0_.tn != '' and f0_.tnvrrab='') or (f0_.tn!='' and f0_.tnvrrab!='' and f0_.tn != f0_.tnvrrab) group by t2.tn;", //
            "INSERT INTO pdpl.digest26(kod_pr,cex,uch,brig,master,gr,tn,fm,opd,kolst,odprps,kateg,pnprof,gost,kod_ur,knceh,kod_sl,knsl,nsl,data_r) SELECT
                         f0_.kod_pr,f0_.cex,f0_.uch,f0_.brig,f0_.master,f0_.gr,t3.tn,
                         IF(t3.fm IS NULL,f0_.fm,CONCAT_WS(' ', CONCAT(upper(left(t3.fm,1)),substring(t3.fm, 2)) , CONCAT(upper(left(t3.ima,1)),substring(t3.ima, 2)), CONCAT(upper(left(t3.oth,1)),substring(t3.oth, 2)))) AS fm,
                         f0_.opd,f0_.kolst,t1.odpr as odprps,f0_.kateg,r1_.pnprof,r1_.gost,r1_.kod_ur,
                         r2_.knceh,r2_.kod_sl,r3_.knsl,r3_.nsl,
                            CASE
                             WHEN (YEAR(FROM_DAYS(DATEDIFF(NOW(), t1.odpr))) > 0) THEN (YEAR(FROM_DAYS(DATEDIFF(NOW(), t1.odpr))) * 12) + MONTH(FROM_DAYS(DATEDIFF(NOW(), t1.odpr)))
                             ELSE ROUND(DATEDIFF(NOW(), t1.odpr) / 30)
                         END 
                         AS data_r
                         FROM
                         pdpl.fschtat f0_
                            INNER JOIN pdpl.r254 r1_ ON ((f0_.kprofpl = r1_.gost))
                            INNER JOIN pdpl.r243ceh r2_ ON ((r2_.kod_ceh = f0_.cex AND r2_.kod_pr = f0_.kod_pr AND r2_.priz_arh = 0))
                            INNER JOIN pdpl.r243sl r3_ ON ((r3_.kod_sl = r2_.kod_sl AND r3_.kod_pr = r2_.kod_pr AND r2_.priz_arh = 0 and r3_.priz_arh = 0))
                            INNER JOIN pdpl.knap t1 on(f0_.tnvrrab = t1.tn and substr(t1.podr,3,8)= f0_.podr and t1.dol = f0_.kprofpl)
                            INNER JOIN pdpl.people t3 on(t3.tn = f0_.tnvrrab and f0_.tnvrrab !='')
                         WHERE
                             f0_.tn != '' OR f0_.tnvrrab != '' group by t1.tn;", //

        ];
        $this->generate($array, null);
    }







    /**
     * ПДПЛ. Справочник 39
     *
     * @param  PDO $pdo
     *
     * @return void
     */
    public function directory39($pdo)
    {
        $this->pdo = $pdo;

        $query_drop = [
            "DROP TABLE IF EXISTS pdpl.digest39;"
        ];

        $query = [
            "CREATE TABLE pdpl.digest39 SELECT 
                t0_.boss,s1_.tn_people,s1_.kod_pr,s1_.cex,s1_.gr,
                s1_.uch,s1_.brig,s1_.master,
                CONCAT(UCASE(LEFT(p2_.fm, 1)),
                        SUBSTRING(p2_.fm, 2),
                        ' ',
                        UPPER(SUBSTR(p2_.ima, 1, 1)),
                        '.',
                        UPPER(SUBSTR(p2_.oth, 1, 1))) AS fm,
                s3_.tn_people AS chiefTn,s3_.gr AS chiefGr,s3_.kod_ur AS chiefUr,
                CONCAT(UCASE(LEFT(p4_.fm, 1)),
                        SUBSTRING(p4_.fm, 2),
                        ' ',
                        UPPER(SUBSTR(p4_.ima, 1, 1)),
                        '.',
                        UPPER(SUBSTR(p4_.oth, 1, 1))) AS chiefFm,
                s3_.kprofpl AS chiefKprofpl,s3_.nprof AS chiefNprof,p2_.tn,
                s1_.kprofpl,s1_.nprof,r5_.kod_ur,r6_.knceh,r7_.knsl
                FROM
                    pdpl.tree t0_
                        INNER JOIN
                    pdpl.staff s1_ ON (t0_.id = s1_.id AND t0_.boss IS NOT NULL)
                        INNER JOIN
                    subordination.people p2_ ON (p2_.tn = s1_.tn_people)
                        INNER JOIN
                    pdpl.staff s3_ ON (s3_.id = t0_.boss AND s3_.kod_ur >= 30
                        AND t0_.boss IS NOT NULL)
                        INNER JOIN
                    subordination.people p4_ ON (p4_.tn = s3_.tn_people)
                        INNER JOIN
                    pdpl.r254 r5_ ON (r5_.gost = s1_.kprofpl)
                        INNER JOIN
                    pdpl.r254 r8_ ON (r8_.gost = s1_.kprofpl)
                        INNER JOIN
                    pdpl.r243ceh r6_ ON (r6_.kod_ceh = s1_.cex
                        AND r6_.priz_arh = 0
                        AND r6_.kod_pr = s1_.kod_pr)
                        INNER JOIN
                    pdpl.r243sl r7_ ON (r7_.kod_sl = r6_.kod_sl
                        AND r7_.kod_pr = r6_.kod_pr
                        AND r7_.priz_arh = 0)
                ORDER BY t0_.boss DESC", //
        ];
        $this->generate($query, $query_drop);
    }

    /**
     * Формирование справочника таблицы редактирования для 
     * Справочник В(30) Итог | Итоговые профессии подразделений с установкой некатегорируемых
     * 
     * Этот метод должен сформировать корректно таблицу editing_digest30 согласно данным из таблицы digest30
     * это необходимо для решения следующих задач:
     * 
     *  - Установка корректных категорий групп для профессий,согласно данным справочника A и данным справочника Б
     *  - Корректное формирование справочника В(Специалисты категорированные и не категорированные получат код группы = 2 после чего специалист УСОТ сделает корректировки)
     *  - Упрощение работы при проведении оценок и управлении группами ШЕ в дальнейшем.
     * 
     * Этот метод должен запускаться после работы класса Modification т.к на момент работы этого метода поля primary key с модификатором AI должны быть сформированы
     * 
     * @todo Было бы хорошо использовать в место прямых указаний первого и шестого символа  кода профессии 
     *  использовать directory_one для обеспечения этой связи между таблицами и согласованностью данных
     * @todo Эйдельман опять поменяла требования,на этот раз необходимо брать всех рабочих т.е первый код профессии 1 а остальные коды не смотреть.
     *  О том что она переобулась сообщил руководству т.к есть письмо где она говорит вообще о других требованиях. тема письма ПДПЛ.Рабочие
     *  
     * @return void
     */
    public function editing_digest30($pdo)
    {
        $this->pdo = $pdo;
        $query_drop = [
            "TRUNCATE TABLE pdpl.editing_digest30;"
        ];
        $query = [
            "INSERT into pdpl.editing_digest30(id_row,kategory_value) SELECT t.id as id_row,
                CASE 
                    when substr(t.gost,1,1)=2 and substr(t.gost,6,1) = 1 and t.kod_ur < 50 then 1
                    when substr(t.gost,1,1)=2 and substr(t.gost,6,1) = 2 then 2
                    when substr(t.gost,1,1)=2 and substr(t.gost,6,1) = 3 then 3
                    when substr(t.gost,1,1)=2 and substr(t.gost,6,1) = 1 and t.kod_ur = 50 then 4
                    when substr(t.gost,1,1)=1 then 5 -- and substr(t.gost,6,1) = 0
                end as kategory_value
                from pdpl.digest30 t;"
        ];
        $this->generate($query, $query_drop);
    }


	    /**
     * ПДПЛ. Справочник 16
     * Справочник 16 является суммирующим для сгенерированных справочников 68, 69 , 26, 30
	 * из-за хотелок Эйдельман пришлось установить жесткие связи со справочниками
     * @param  PDO $pdo
     *
     * @return void
     */
    public function directory16($pdo)
    {
        $this->pdo = $pdo;

        $query_drop = [
            "DROP TABLE IF EXISTS pdpl.digest16;"
        ];

        $query = [
            "CREATE TABLE pdpl.digest16 (id INT AUTO_INCREMENT PRIMARY KEY) SELECT t0.kod_pr, t1.knsl, t0.kod_ceh,t0.knceh, count(t3.cont) as gshe_count, sum(t3.col) as gshe_st, sum(t3.gshe_total) as gshe_total, coalesce(t26.cont, 0) + t2.gsot_count as all_count, t2.gsot_count, t2.gsot_st, t2.gsot_total, coalesce(ed16.value,1.000) as increase, round(sum(t3.gshe_total) * coalesce(ed16.value,1.000)) as gshe, round(t2.gsot_total * coalesce(ed16.value,1.000)) as gsot  FROM pdpl.r243ceh t0
				left join pdpl.r243sl t1 on t1.kod_sl = t0.kod_sl and t0.kod_pr=t1.kod_pr
                left join (SELECT t0.kod_pr,t0.cex,t0.knceh, count(t0.id) as gsot_count, sum(t0.kolst) as gsot_st, sum(t0.total_s) as gsot_total
								FROM pdpl.digest68 t0
								JOIN pdpl.digest30 t1 on substring(t0.gost,1,5) = t1.proff
								LEFT JOIN pdpl.editing_digest30 t5 on t5.id_row = t1.proff
								LEFT JOIN pdpl.directory_one t4 on
									(t4.first != 1 and t4.first = substring(t0.gost,1,1) and substring(t0.gost,6,1) = t4.sixth and t4.code_group = t5.kategory_value ) or
									(t4.first = 1  and t4.code_group = t5.kategory_value )
								JOIN pdpl.directory_one tt4 on
									(tt4.first != 1 and tt4.first = substring(t0.gost,1,1) and substring(t0.gost,6,1) = tt4.sixth and tt4.code_group = t1.base_kategory ) or
									(tt4.first = 1  and tt4.code_group = t1.base_kategory )       
								JOIN pdpl.digest28 t2 on t2.kateg = t0.kateg
								JOIN pdpl.editing_digest28 t3 on t3.id_row = t2.id
								JOIN pdpl.digest0 t6 on t6.parameter = 'eva_min_period' and t0.data_r > t6.value
								WHERE (t3.Chief = 1 or t3.specialists_categorized = 1 or t3.specialists_uncategorized = 1 or t3.employees = 1
									or t3.heads_of_departments = 1 or t3.workers = 1 and t5.kategory_value in(1,2,3,4,5)) and t0.kod_ur < 50
										group by t0.kod_pr, t0.cex) t2 on t2.kod_pr = t0.kod_pr and t2.cex = t0.kod_ceh
				left join (SELECT t0.id, t0.kod_pr,t0.cex,t0.knceh, count(t0.knceh) as cont, sum(t0.kolst) as col, sum(t0.total_s) as gshe_total
							FROM pdpl.digest69 t0
								JOIN pdpl.digest30 t1 on substring(t0.gost,1,5) = t1.proff
								LEFT JOIN pdpl.editing_digest30 t5 on t5.id_row = t1.proff
								JOIN pdpl.directory_one tt4 on
								(tt4.first != 1 and tt4.first = substring(t0.gost,1,1) and substring(t0.gost,6,1) = tt4.sixth and tt4.code_group = t1.base_kategory ) or
								(tt4.first = 1  and tt4.code_group = t1.base_kategory )  
							where t0.kod_ur < 50
							group by t0.kod_pr,t0.cex, t0.opd,t0.podr,t0.nom_pol) t3 on t3.kod_pr = t0.kod_pr and t3.cex = t0.kod_ceh 
				left join (SELECT distinct t0.kod_pr,t0.cex,t0.knceh, count(t0.knceh) as cont 
								FROM pdpl.digest26 t0 
										JOIN pdpl.digest30 t1 on substring(t0.gost,1,5) = t1.proff
									JOIN pdpl.digest0 t6 on (t6.Parameter = 'eva_min_period' and t0.data_r <= t6.value or t0.missing = 1) and t0.kod_ur < 50
								group by t0.kod_pr, t0.cex ) as t26 on t26.kod_pr = t0.kod_pr and t26.cex = t0.kod_ceh
				left join pdpl.editing_digest16 as ed16 on 	ed16.cex = t0.kod_ceh and ed16.kod_pr=t0.kod_pr 
				 where t0.priz_arh=0 and (t0.kod_sl NOT LIKE '91' AND t0.kod_pr IN ('01' , '02', '03'))
				 group by t0.kod_pr, t0.kod_ceh
				 HAVING count(t3.knceh) > 0;", //
        ];
        $this->generate($query, $query_drop);
    }

    public function sprTarif($pdo)
    {
        $this->pdo = $pdo;

        $query_drop = [
            "DROP TABLE IF EXISTS pdpl.spr_tarif;"
        ];

        $query = [
            "CREATE TABLE pdpl.spr_tarif (id INT AUTO_INCREMENT PRIMARY KEY) 
            SELECT f.kod_pr, f.fm , f.tn, f.cex, f.uch, f.master, f.brig, f.kateg, f.kprofpl,
            CASE 
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig=CONCAT(f.master,f.brig) and kprofpl = f.kprofpl) > 0 
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig=CONCAT(f.master,f.brig) and kprofpl = f.kprofpl) 
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig=CONCAT(f.master,f.brig) and kprofpl = '') > 0
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig=CONCAT(f.master,f.brig) and kprofpl = '') 
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig='' and kprofpl = '') > 0
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu=f.uch and kod_brig='' and kprofpl = '')
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu='' and kod_brig='' and kprofpl = '') > 0
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh=f.cex and kod_admu='' and kod_brig='' and kprofpl = '')
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh='' and kod_admu='' and kod_brig='' and kprofpl = '') > 0
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr=f.kod_pr and kod_ceh='' and kod_admu='' and kod_brig='' and kprofpl = '')
            WHEN (SELECT count(n_cts) FROM pdpl.spr_alg where kateg = f.kateg and kod_pr='' and kod_ceh='' and kod_admu='' and kod_brig='' and kprofpl = '') > 0
                    THEN (SELECT n_cts FROM pdpl.spr_alg where kateg = f.kateg and kod_pr='' and kod_ceh='' and kod_admu='' and kod_brig='' and kprofpl = '')
            WHEN (f.kateg in ('41','42','51','52','61','62'))
                    THEN '20'
            WHEN (f.kateg in ('09','11','12'))
                    THEN '21'
            END
            as tarif 
            FROM pdpl.fschtat f where substring(f.kprofpl,1, 1)='2';", //
        ];
        $this->generate($query, $query_drop);
    }

    /**
     * Метод выполняющий подготовленные запросы
     *
     * @param array $query - массив конфигурации
     * @param array $query_drop - массив для удаления таблиц
     * @return void
     */
    private function generate($query, $query_drop = null)
    {
        try {
            if (!is_null($query_drop)) {
                foreach ($query_drop as $drop) {
                    $this->pdo->exec($drop);
                }
            }
            foreach ($query as $request) {
                $this->PDOmap->PDOAssert($this->pdo->exec($request), "Error Generate table = " . $request);
            }
        } catch (Exception $e) {
            echo $e->getMessage();
            $this->PDOmap->loger($e, $e->getMessage());
        }
    }
}

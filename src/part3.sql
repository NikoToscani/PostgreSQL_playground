------------------------ex01------------------------
CREATE OR REPLACE FUNCTION transferredpoints_output()
RETURNS TABLE (
    "Peer1" VARCHAR,
    "Peer2" VARCHAR,
    "PointsAmount" INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT t1.checkingpeer AS Peer1, t1.checkedpeer AS Peer2, t1.pointsamount AS PointsAmount
    FROM transferredpoints AS t1;
    -- UNION ALL
    -- SELECT t2.checkedpeer, t2.checkingpeer, -t2.pointsamount
    -- FROM transferredpoints AS t2;
END;
$$ LANGUAGE plpgsql;

------------------------ex02------------------------
CREATE OR REPLACE FUNCTION peer_task_xp_output()
RETURNS TABLE (
    "Peer" VARCHAR,
    "Task" VARCHAR,
    "XP" BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT (SELECT checks.peer
            FROM checks
            WHERE checks.id = xp."Check"),
           (SELECT checks.task
            FROM checks
            WHERE checks.id = xp."Check"),
            xp.xpamount
    FROM xp
    WHERE xp."Check" IN (SELECT p2p."Check"
                            FROM p2p
                            WHERE p2p."State" = 'Success')
    AND (xp."Check" NOT IN (SELECT verter."Check" FROM verter) OR
         xp."Check" IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Success'));
END;
$$ LANGUAGE plpgsql;

------------------------ex03------------------------
CREATE OR REPLACE FUNCTION whole_day_in_output(p_date DATE)
RETURNS TABLE ("Peer" VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT t1.peer
    FROM (SELECT * FROM timetracking WHERE timetracking."State" = '1') AS t1
    JOIN (SELECT * FROM timetracking WHERE timetracking."State" = '2') AS t2
    ON t1.peer = t2.peer AND t2.id = t1.id + 1
    WHERE (t2."Date" + t2."Time") - (t1."Date" + t1."Time") > '1 day'
    AND p_date BETWEEN t1."Date" AND t2."Date";
END;
$$ LANGUAGE plpgsql;

------------------------ex04------------------------
CREATE OR REPLACE FUNCTION points_change_output_1()
RETURNS TABLE (
    "Peer" VARCHAR,
    "PointsChange" BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT t1.checkingpeer AS "Peer", COALESCE("PointsChange1",0) - COALESCE("PointsChange2",0) AS "PointsChange"
    FROM
    (SELECT TransferredPoints.checkingpeer, SUM(TransferredPoints.pointsamount) AS "PointsChange1"
    FROM TransferredPoints
    GROUP BY TransferredPoints.checkingpeer) AS t1
    LEFT JOIN
    (SELECT TransferredPoints.checkedpeer, SUM(TransferredPoints.pointsamount) AS "PointsChange2"
    FROM TransferredPoints
    GROUP BY TransferredPoints.checkedpeer) AS t2
    ON t1.checkingpeer = t2.checkedpeer
    ORDER BY "PointsChange" DESC;
END;
$$ LANGUAGE plpgsql;

------------------------ex05------------------------
CREATE OR REPLACE FUNCTION points_change_output_2()
RETURNS TABLE (
    "Peer" VARCHAR,
    "PointsChange" BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT t1."Peer1" AS "Peer", COALESCE("PointsChange1",0) - COALESCE("PointsChange2",0) AS "PointsChange"
    FROM
    (SELECT "Peer1", SUM("PointsAmount") AS "PointsChange1"
    FROM (SELECT * FROM transferredpoints_output()) AS f1
    GROUP BY "Peer1") AS t1
    LEFT JOIN
    (SELECT "Peer2", SUM("PointsAmount") AS "PointsChange2"
    FROM (SELECT * FROM transferredpoints_output()) AS f2
    GROUP BY "Peer2") AS t2
    ON t1."Peer1" = t2."Peer2"
    ORDER BY "PointsChange" DESC;
END;
$$ LANGUAGE plpgsql;

------------------------ex06------------------------
CREATE OR REPLACE FUNCTION most_freq_check_task()
RETURNS TABLE (
    "Day" DATE,
    "Task" VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT t1."Date", t2.task
    FROM
    (SELECT DISTINCT ON (checks."Date") checks."Date", COUNT(*)
    FROM checks
    GROUP BY checks."Date", checks.task
    ORDER BY checks."Date", COUNT(checks.task) DESC) AS t1
    JOIN
    (SELECT checks."Date", checks.task, COUNT(*)
    FROM checks
    GROUP BY checks."Date", checks.task) AS t2
    ON t1."Date" = t2."Date" AND t1.count  = t2.count
    ORDER BY t1."Date", t2.task;
END;
$$ LANGUAGE plpgsql;

------------------------ex07------------------------
CREATE OR REPLACE PROCEDURE peer_all_block(
    IN ref REFCURSOR,
    IN p_block VARCHAR
)
LANGUAGE PLPGSQL
AS $$
DECLARE
    num_tasks INT;
BEGIN
    SELECT COUNT(*) INTO num_tasks
    FROM (
        SELECT *
        FROM tasks
        WHERE title ~ (''||p_block||'[0-9]')
    ) AS num_rows;

    OPEN ref FOR
    
    SELECT "Peer", MAX(t2.max) AS "Day", COUNT(*) FROM (
        SELECT peer AS "Peer", task, MAX("Date") FROM checks
        JOIN xp ON checks.id = xp."Check"
        JOIN (SELECT * FROM tasks WHERE title ~ (''||p_block||'[0-9]')) AS t1 ON task = t1.title
        GROUP BY peer, task
        ORDER BY peer, task
    ) AS t2
    GROUP BY "Peer"
    HAVING COUNT(*) = num_tasks
    ORDER BY "Day";

END;
$$;

/* Проверка */
BEGIN;
CALL peer_all_block('ref', 'A');
FETCH ALL IN "ref";
COMMIT;

BEGIN;
CALL peer_all_block('ref', 'SQL');
FETCH ALL IN "ref";
COMMIT;

------------------------ex08------------------------

/* не было общих данных в таблицах, пришлось обновить, случайным образом добавив данные из одной таблицы в другую */
/* обновление первой колонки */
-- update recommendations set (peer) =
--   (select t.peer2
--    from (SELECT f1.peer1, f1.peer2 FROM friends f1
--          UNION
--          SELECT DISTINCT ON (f2.peer1) f2.peer2, f2.peer1 FROM friends f2) AS t
--    order by random()+recommendations.id limit 1);
/* обновление второй колонки */
-- update recommendations set (recommendedpeer)=
--   (select peer1 from friends
--    where id between 1 and 50
--    order by random()+recommendations.id limit 1);

CREATE OR REPLACE FUNCTION recommended_peer()
RETURNS TABLE (
    "Peer" VARCHAR,
    "RecommendedPeer" VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (t2.peer1) t2.peer1 AS "Peer", t2.recommendedpeer AS "RecommendedPeer"
    FROM (SELECT t1.peer1, t1.recommendedpeer, COUNT(*)
        FROM (SELECT * FROM friends
                JOIN recommendations ON friends.peer2 = recommendations.peer
                AND friends.peer1 <> recommendations.recommendedpeer) AS t1
        GROUP BY t1.peer1, t1.recommendedpeer) AS t2
    ORDER BY t2.peer1, t2.count DESC;
END;
$$ LANGUAGE plpgsql;

------------------------ex09------------------------
CREATE OR REPLACE FUNCTION percentage_of_started_block_output(
    p_block1 VARCHAR
    , p_block2 VARCHAR
) RETURNS TABLE (
    "StartedBlock1" INT
    , "StartedBlock2" INT
    , "StartedBothBlocks" INT
    , "DidntStartAnyBlock" INT
) AS $$
DECLARE
    total_num_peers INT;
    num_peers_1 INT;
    num_peers_2 INT;
    num_peers_1_2 INT;
BEGIN
    SELECT COUNT(*) INTO total_num_peers FROM peers;

    CREATE TEMP TABLE IF NOT EXISTS tt_block1 AS
    SELECT DISTINCT ON (peer) * FROM checks
    WHERE task ~ (''||p_block1||'[0-9]');

    CREATE TEMP TABLE IF NOT EXISTS tt_block2 AS
    SELECT DISTINCT ON (peer) * FROM checks
    WHERE task ~ (''||p_block2||'[0-9]');

    SELECT COUNT(*) INTO num_peers_1 FROM tt_block1;
    SELECT COUNT(*) INTO num_peers_2 FROM tt_block2;
    SELECT COUNT(*) INTO num_peers_1_2 FROM 
    (SELECT * FROM tt_block1 JOIN tt_block2
    ON tt_block1.peer = tt_block2.peer) AS t;

    RETURN QUERY

        SELECT num_peers_1*100/total_num_peers
               , num_peers_2*100/total_num_peers
               , num_peers_1_2*100/total_num_peers
               , (total_num_peers-num_peers_1_2)*100/total_num_peers
               ;

    DROP TABLE tt_block1;
    DROP TABLE tt_block2;
END;
$$ LANGUAGE plpgsql;

------------------------ex10------------------------
CREATE OR REPLACE FUNCTION bd_succ_fail_output(
) RETURNS TABLE (
    "SuccessfulChecks" INT
    , "UnsuccessfulChecks" INT
) AS $$
DECLARE
    num_bd_check_peers INT;
    num_bd_check_succ INT;
    num_bd_check_fail INT;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS birth_checks AS
    SELECT * FROM checks
    JOIN peers ON checks.peer = peers.nickname
    AND TO_CHAR(checks."Date", 'mm-dd') = TO_CHAR(peers.birthday, 'mm-dd')
    AND checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Success' OR p2p."State" = 'Failure');

    SELECT COUNT(*) INTO num_bd_check_peers FROM birth_checks;
    SELECT COUNT(*) INTO num_bd_check_succ FROM (
    SELECT * FROM birth_checks
    WHERE birth_checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Success')
    AND (birth_checks.id NOT IN (SELECT verter."Check" FROM verter) OR
    birth_checks.id IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Success'))) AS t1;
    SELECT COUNT(*) INTO num_bd_check_fail FROM (
    SELECT * FROM birth_checks
    WHERE birth_checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Failure')
    OR birth_checks.id IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Failure')) AS t1;

    RETURN QUERY

        SELECT num_bd_check_succ*100/num_bd_check_peers
               , num_bd_check_fail*100/num_bd_check_peers
               ;

    DROP TABLE birth_checks;
END;
$$ LANGUAGE plpgsql;

------------------------ex11------------------------
CREATE OR REPLACE PROCEDURE peer_vs_tasks(
    IN ref REFCURSOR
    , IN p_task1 VARCHAR
    , IN p_task2 VARCHAR
    , IN p_task3 VARCHAR
)
LANGUAGE PLPGSQL
AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS task1 ON COMMIT DROP AS
    SELECT * FROM checks
    WHERE task = p_task1 AND checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Success')
    AND (checks.id NOT IN (SELECT verter."Check" FROM verter) OR
    checks.id IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Success'));

    CREATE TEMP TABLE IF NOT EXISTS task2 ON COMMIT DROP AS
    SELECT * FROM checks
    WHERE task = p_task2 AND checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Success')
    AND (checks.id NOT IN (SELECT verter."Check" FROM verter) OR
    checks.id IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Success'));

    CREATE TEMP TABLE IF NOT EXISTS task3 ON COMMIT DROP AS
    SELECT peer FROM checks WHERE task = p_task3
    EXCEPT                                                  
    SELECT peer FROM (
        SELECT * FROM checks
        WHERE task = p_task3 AND checks.id IN (SELECT p2p."Check" FROM p2p WHERE p2p."State" = 'Success')
        AND (checks.id NOT IN (SELECT verter."Check" FROM verter) OR
        checks.id IN (SELECT verter."Check" FROM verter WHERE verter."State" = 'Success'))
    ) AS t1;

    OPEN ref FOR
    
    SELECT task1.peer AS "Peer" FROM task1
    JOIN task2 ON task1.peer = task2.peer
    JOIN task3 ON task1.peer = task3.peer;

END;
$$;

/* Проверка */
BEGIN;
CALL peer_vs_tasks('ref', 'SQL3', 'CPP7', 'AP2');
FETCH ALL IN "ref";
COMMIT;

------------------------ex12------------------------
CREATE OR REPLACE FUNCTION num_preciding_tasks(
) RETURNS TABLE (
    "Task" VARCHAR
    , "PrevCount" INT
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE cte(title, id) AS (
        SELECT title, 0 AS id FROM tasks WHERE parenttask IS NULL
        UNION ALL
        SELECT tasks.title, id + 1 AS id FROM tasks
        JOIN cte
        ON cte.title = tasks.parenttask
    )
    SELECT * FROM cte;
END;
$$ LANGUAGE plpgsql;
------------------------ex13------------------------
/* удаление лишней записи в таблице xp */
delete from xp where id = 63;

CREATE OR REPLACE PROCEDURE lucky_days(
    IN ref REFCURSOR
    , IN p_num INT
)
LANGUAGE PLPGSQL
AS $$
BEGIN

    OPEN ref FOR
    
    /* 1. составить табличку (cte) из checks и p2p, где p2p содержит только время Start
    и включить в нее checks: id, Date, p2p: Time, State, id */
    WITH base AS (
        select c.id, c.task, c."Date" as b_date, p."Time" as b_start_time from checks as c
        join (select * from p2p where (p2p."State" = 'Start')) as p
        on (c.id = p."Check")
    )
    /* 2. составить табличку (cte) из base и p2p, где p2p содержит только строки не-Start
    и включить в нее все из base и p2p: State */
    , base_n_p2p AS (
        select base.*, p."State" AS p2p_state from base
        join (select * from p2p where ("State" != 'Start')) as p
        on (base.id = p."Check")
    )
    /* 3. составить табличку (cte) из base_n_p2p и verter, где verter содержит только строки не-Start,
    замещает все пустые строки (проверка verter'ом отсутствует) на 'Success',
    вычисляет результат S_or_F1 (Success или Failure) на основе значений p2p_state на v_state,
    и включить в нее base_n_p2p: id, b_date, b_start_time и S_or_F1 */
    , b_p2p_verter AS (
        select base_n_p2p.id, task, b_date, b_start_time
        , (select case when
            p2p_state = coalesce(v."State", 'Success') then 'Success' else 'Failure'
        end) AS S_or_F1
        from base_n_p2p
        left join (select * from verter where ("State" != 'Start')) as v
        on (base_n_p2p.id = v."Check")
    )
    /* 4. составить табличку (cte) из b_p2p_verter, xp и tasks */
    , p2p_verter_xp_task AS (
        select b_p2p_verter.id, b_p2p_verter.task, b_p2p_verter.b_date, b_p2p_verter.b_start_time, b_p2p_verter.S_or_F1
        , (select case when
            coalesce(xp.xpamount, 0)*100/tasks.maxxp > 80 then 'Success' else 'Failure'
        end) AS S_or_F2
        from b_p2p_verter
        left join xp on (b_p2p_verter.id = xp."Check")
        join tasks on (b_p2p_verter.task = tasks.title)
    )
    /* 5. вернуть табличку (cte) из p2p_verter_xp_task со статусом Success/Failure
    на основании колонок S_or_F1 и S_or_F2*/
    , complete_pvxt AS (
        select p2p_verter_xp_task.id, p2p_verter_xp_task.task, p2p_verter_xp_task.b_date, p2p_verter_xp_task.b_start_time
        , (select case when s_or_f1 = s_or_f2 AND s_or_f1 = 'Success' then 'Success' else 'Failure' end) AS S_or_F
        from p2p_verter_xp_task
    )
    /* 6. добавить в табличку b_p2p_verter колонку с вычисленным кодом-числовым значением, соответствующим 
    каждому непрерывному значению 'Success' или 'Failure' (S_or_F) с помощью оконной функции row_number() */
    , num_of_contin_state_val AS (
        select complete_pvxt.*,
        row_number() over (partition by b_date order by b_start_time) -
        row_number() over (partition by b_date, s_or_f order by b_start_time) as code
        from complete_pvxt
    )
    /* 7. объединим (group by) таблицу num_of_contin_state_val по трем колонкам: b_date, s_or_f, code */
    , final_groupped_table AS (
        select b_date, s_or_f, code, count(*) as quantity from num_of_contin_state_val
        group by b_date, s_or_f, code order by b_date
    )
    select b_date
    from final_groupped_table
    where s_or_f = 'Success' and quantity = p_num;

END;
$$;

/* Проверка */
BEGIN;
CALL lucky_days('ref', 2);
FETCH ALL IN "ref";
COMMIT;

------------------------ex15------------------------
CREATE OR REPLACE PROCEDURE came_before_time(
    IN ref REFCURSOR
    , IN p_time VARCHAR
    , IN p_num INT
)
LANGUAGE PLPGSQL
AS $$
BEGIN
    OPEN ref FOR
    select peer
    from (
        select peer, count(*)
        from timetracking
        where "State" = '1' and "Time" < p_time::time
        group by peer
    ) as t 
    where count > p_num;
END;
$$;

/* Проверка */
BEGIN;
CALL came_before_time('ref', '12:00:00', 3);
FETCH ALL IN "ref";
COMMIT;
------------------------ex16------------------------
CREATE OR REPLACE PROCEDURE left_more_then_times(
    IN ref REFCURSOR
    , IN p_num_days INT
    , IN p_num_times INT
)
LANGUAGE PLPGSQL
AS $$
BEGIN
    OPEN ref FOR
    select peer
    from (
        select peer, count(*)
        from timetracking
        where "State" = '2' AND "Date" > current_date-p_num_days
        group by peer
    ) as t 
    where count > p_num_times;
END;
$$;

/* Проверка */
BEGIN;
CALL left_more_then_times('ref', 410, 5);
FETCH ALL IN "ref";
COMMIT;

------------------------ex17------------------------
CREATE OR REPLACE FUNCTION early_entries(
) RETURNS TABLE (
    "Month" TEXT
    , "EarlyEntries" BIGINT
) AS $$
BEGIN
    RETURN QUERY
    with tt_vs_peers AS (
        select * from (select * from timetracking where "State" = '1') t
        join peers p on t.peer = p.nickname
        where to_char(t."Date"::date, 'Month') = to_char(p.birthday::date, 'Month')
    )
    , total_num_entries AS (
        select to_char("Date"::date, 'MM') as t_mon_num
               , count(*)
        from tt_vs_peers
        group by t_mon_num
    )
    , early_entries AS (
        select to_char(t."Date"::date, 'MM') as e_mon_num
               , count(*)
        from (select * from tt_vs_peers where "Time" < '12:00:00') as t
        group by e_mon_num
    )
    select to_char(to_date(t_mon_num::text, 'MM'), 'Month')
           , e.count*100/t.count AS percent
    from total_num_entries t
    join early_entries e on t_mon_num = e_mon_num
    order by t_mon_num;
END;
$$ LANGUAGE plpgsql;

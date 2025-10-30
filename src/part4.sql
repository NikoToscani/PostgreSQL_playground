/****************************************************************************/
/*																			*/
/*						УДАЛЕНИЕ ОБЪЕКТОВ БД ИЗ PART1						*/
/*																			*/
/****************************************************************************/

DROP TRIGGER IF EXISTS trg_Tasks ON Tasks;
DROP TRIGGER IF EXISTS trg_p2p ON P2P;
DROP TRIGGER IF EXISTS trg_verter ON Verter;
DROP TRIGGER IF EXISTS trg_p2p_update_points ON P2P;

DROP TABLE IF EXISTS Peers CASCADE;
DROP TABLE IF EXISTS Verter CASCADE;
DROP TABLE IF EXISTS Tasks CASCADE;
DROP TABLE IF EXISTS Friends CASCADE;
DROP TABLE IF EXISTS Checks CASCADE;
DROP TABLE IF EXISTS TransferredPoints CASCADE;
DROP TABLE IF EXISTS P2P CASCADE;
DROP TABLE IF EXISTS XP CASCADE;
DROP TABLE IF EXISTS TableNameTimeTracking CASCADE;
DROP TABLE IF EXISTS TableName_Recommendations CASCADE;

DROP FUNCTION IF EXISTS single_root();
DROP FUNCTION IF EXISTS only_one_start_p2p();
DROP FUNCTION IF EXISTS only_one_start_verter();
DROP FUNCTION IF EXISTS p2p_update_points();

/* специально созданные функции: скалярные и не скалярные с параметрами и без */
DROP FUNCTION IF EXISTS scalar_func_1(VARCHAR);
DROP FUNCTION IF EXISTS scalar_func_2();
DROP FUNCTION IF EXISTS scalar_func_3(INT);
DROP FUNCTION IF EXISTS nonscalar_func_1();

DROP PROCEDURE IF EXISTS import_from_csv(TEXT,TEXT,TEXT);
DROP PROCEDURE IF EXISTS export_to_csv(TEXT,TEXT,TEXT);

DROP TYPE IF EXISTS status CASCADE;

/****************************************************************************/
/*																			*/
/*							СОЗДАНИЕ ОБЪЕКТОВ БД							*/
/*																			*/
/****************************************************************************/

CREATE TYPE status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS Peers(
	Nickname VARCHAR PRIMARY KEY NOT NULL,
	Birthday DATE
);

CREATE TABLE IF NOT EXISTS Tasks(
	Title VARCHAR PRIMARY KEY NOT NULL UNIQUE,
	ParentTask VARCHAR,
	MaxXP INT NOT NULL,
	CONSTRAINT fk_parent_task FOREIGN KEY (ParentTask) REFERENCES Tasks(Title),
	CONSTRAINT unique_pair CHECK (ParentTask != Title)
);

CREATE TABLE IF NOT EXISTS Friends (
  	ID SERIAL PRIMARY KEY,
	Peer1 VARCHAR NOT NULL,
	Peer2 VARCHAR NOT NULL,
	CONSTRAINT fk_friends_peer1 FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
	CONSTRAINT fk_friends_peer2 FOREIGN KEY (Peer2) REFERENCES Peers(Nickname),
	CONSTRAINT unique_pair CHECK (Peer1 != Peer2)
);

CREATE TABLE IF NOT EXISTS Checks (
	ID SERIAL PRIMARY KEY,
	Peer VARCHAR NOT NULL,
	Task VARCHAR NOT NULL,
	"Date" DATE,
	CONSTRAINT fk_checks_peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
	CONSTRAINT fk_checks_task FOREIGN KEY (Task) REFERENCES Tasks(Title)
);

CREATE TABLE IF NOT EXISTS P2P(
	ID SERIAL PRIMARY KEY,
	"Check" BIGINT NOT NULL,
	CheckingPeer VARCHAR NOT NULL,
	"State" status,
	"Time" TIME NOT NULL,
	CONSTRAINT fk_p2p_checking_peer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
	FOREIGN KEY ("Check") REFERENCES Checks(id)
);

CREATE TABLE IF NOT EXISTS Verter(
	ID SERIAL PRIMARY KEY,
	"Check" BIGINT NOT NULL,
	"State" status,
	"Time" TIME NOT NULL,
	FOREIGN KEY ("Check") REFERENCES Checks(id)
);

CREATE TABLE IF NOT EXISTS TransferredPoints (
	ID SERIAL PRIMARY KEY,
	CheckingPeer VARCHAR NOT NULL,
	CheckedPeer VARCHAR NOT NULL,
	PointsAmount INT NOT NULL DEFAULT 0,
	CONSTRAINT unique_pair CHECK (CheckingPeer != CheckedPeer),
	CONSTRAINT fk_checking_peer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
	CONSTRAINT fk_checked_peer FOREIGN KEY (CheckedPeer) REFERENCES Peers(Nickname)						 
);

CREATE TABLE IF NOT EXISTS XP (
	ID SERIAL PRIMARY KEY,
	"Check" BIGINT NOT NULL,
	XPAmount BIGINT NOT NULL,
	CONSTRAINT fk_check_id FOREIGN KEY ("Check") REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS TableNameTimeTracking (
  ID SERIAL PRIMARY KEY,
  Peer VARCHAR NOT NULL,
  "Date" DATE NOT NULL,
  "Time" TIME WITHOUT TIME ZONE NOT NULL,
  "State" VARCHAR NOT NULL,
  CONSTRAINT valid_state CHECK ("State" IN ('1', '2')),
  CONSTRAINT fk_peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS TableName_Recommendations (
	ID SERIAL PRIMARY KEY,
	Peer VARCHAR NOT NULL,
	RecommendedPeer VARCHAR NOT NULL,
	CONSTRAINT fk_peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
	CONSTRAINT fk_recommendedpeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname),
	CONSTRAINT unique_pair CHECK (Peer != RecommendedPeer)
);

/****************************************************************************/
/*																			*/
/*						ТРИГГЕРНЫЕ ФУНКЦИИ И ТРИГГЕРЫ						*/
/*																			*/
/****************************************************************************/

CREATE OR REPLACE FUNCTION single_root() -- срабатывает при новой записи в Tasks
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.ParentTask IS NULL THEN -- если у новой записи нуливой ParentTask
		IF EXISTS (
			SELECT * 				 -- если до обновления в таблице обнаружена
			FROM Tasks					/* запись с нуливой ParentTask */
			WHERE Title = NEW.Title AND ParentTask IS NULL
		)
		THEN RAISE EXCEPTION 'There can be only one root task'; -- сообщение об ошибке и прерывание операции
		END IF;
	END IF;
	IF NEW.ParentTask IS NOT NULL THEN -- если у новой записи ненуливой ParentTask
		IF NOT EXISTS (
			SELECT * 				 -- если до обновления в таблице не обнаружена
			FROM Tasks					/* запись с нуливой ParentTask */
			WHERE ParentTask IS NULL
		)
		THEN RAISE EXCEPTION 'There is no root task yet'; -- сообщение об ошибке и прерывание операции
		END IF;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_Tasks -- срабатывает при новой записи в Tasks
BEFORE INSERT OR UPDATE ON Tasks
FOR EACH ROW
EXECUTE FUNCTION single_root();


CREATE OR REPLACE FUNCTION only_one_start_p2p()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW."State" = 'Start' THEN
		IF EXISTS (
			SELECT * 
			FROM P2P
			WHERE id = NEW.id AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check already hAS Start status';
		END IF;
	END IF;
	IF NEW."State" IN ('Success', 'Failure') THEN
		IF NEW."Time" < (SELECT "Time"
					   FROM P2P
					   WHERE NEW."Check" = P2P."Check"
					   ORDER BY "Time" DESC
					   LIMIT 1) THEN
		RAISE EXCEPTION 'Check can NOT be finished earlier then it''''s start';
		END IF;
		IF NOT EXISTS (
			SELECT * 
			FROM P2P
			WHERE "Check" = NEW."Check" AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check doesn''''t have Start status';
		END IF;
	END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p
BEFORE INSERT OR UPDATE ON P2P
FOR EACH ROW
EXECUTE FUNCTION only_one_start_p2p();


CREATE OR REPLACE FUNCTION only_one_start_verter()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW."State" = 'Start' THEN
		IF EXISTS (
			SELECT * 
			FROM Verter
			WHERE id = NEW.id AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check already hAS Start status';
		END IF;
	END IF;
	IF NEW."State" IN ('Success', 'Failure') THEN
		IF NEW."Time" < (SELECT "Time"
					   FROM Verter
					   WHERE NEW."Check" = Verter."Check"
					   ORDER BY "Time" DESC
					   LIMIT 1) THEN
		RAISE EXCEPTION 'Check can NOT be finished earlier then it''''s start';
		END IF;
		IF NOT EXISTS (
			SELECT * 
			FROM Verter
			WHERE "Check" = NEW."Check" AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check doesn''''t have Start status';
		END IF;
	END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_verter
BEFORE INSERT OR UPDATE ON Verter
FOR EACH ROW
EXECUTE FUNCTION only_one_start_verter();


CREATE OR REPLACE FUNCTION p2p_update_points()
RETURNS TRIGGER AS $$
DECLARE
  peer_value VARCHAR;
BEGIN
  IF NEW."State" IN ('Success', 'Failure') THEN
    BEGIN
      SELECT Peer INTO peer_value
      FROM Checks					-- поиск соответствующего пира в проверках Checks
      WHERE Checks.ID = NEW."Check"  /* и присваивание переменной peer_value */
      LIMIT 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN  -- если такой пир не найден
        RAISE NOTICE 'No corresponding Peer found for Check ID %', NEW."Check";
        RETURN NEW;            -- сообщение об ошибке (выше) и запись в P2P            ???
  						/* Нет, возвращаемое значение для строчного триггера AFTER всегда игнорируется */
    END;
	IF EXISTS (
		SELECT *
		FROM TransferredPoints
		WHERE NEW.CheckingPeer = TransferredPoints.CheckingPeer
		AND peer_value = TransferredPoints.CheckedPeer
	) THEN 
    	UPDATE TransferredPoints
    	SET PointsAmount = PointsAmount + 1
    	WHERE NEW.CheckingPeer = TransferredPoints.CheckingPeer
		AND peer_value = TransferredPoints.CheckedPeer;
	ELSE 
		INSERT INTO TransferredPoints(CheckingPeer,CheckedPeer,PointsAmount)
		VALUES(NEW.CheckingPeer, 
				(SELECT Peer
				FROM Checks
				WHERE Checks.ID = NEW."Check"), -- WHERE Checks.ID = NEW."Check"), -- ???
			  	1); /* надо ли сразу вносить 1, т.к. если функиця активировалсь, то одна проверка P2P уже 
				       завершилась, соответственно 1 трансфер пирпоинта должен произойти */
	END IF;
  END IF;
  RETURN NEW; -- вносит ли эта выражение еще одну запись P2P? 
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_update_points
AFTER INSERT ON P2P
FOR EACH ROW
EXECUTE FUNCTION p2p_update_points();

/****************************************************************************/
/*																			*/
/* 					ПРОЦЕДУРЫ ЗАГРУЗКИ/ВЫГРУЗКИ CSV-ФАЙЛОВ 					*/
/*																			*/
/****************************************************************************/

SET datestyle = dmy;

CREATE OR REPLACE PROCEDURE import_from_csv(
    IN p_table_name TEXT,
    IN p_file_path TEXT,
	IN p_file_delim TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
	IF p_table_name = 'P2P' OR p_table_name = 'p2p' THEN
		CREATE TABLE temp_P2P(ID SERIAL PRIMARY KEY, "Check" BIGINT NOT NULL, CheckingPeer VARCHAR NOT NULL,
			state_num INT NOT NULL,	"Time" TIME NOT NULL,
			CONSTRAINT fk_p2p_checking_peer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname)
		);
		EXECUTE 'COPY temp_p2p FROM ''' || p_file_path || ''' WITH NULL AS ''None'' delimiter ''' || p_file_delim || ''' CSV HEADER;';

		INSERT INTO P2P
		SELECT id, "Check", checkingpeer, (enum_range(NULL::status))[state_num+1], "Time"
		FROM temp_P2P;

		DROP TABLE temp_P2P;
	ELSIF p_table_name = 'VERTER' OR p_table_name = 'verter' OR p_table_name = 'Verter' THEN
		CREATE TABLE temp_verter(ID SERIAL PRIMARY KEY,	"Check" BIGINT NOT NULL, state_num INT NOT NULL, "Time" TIME NOT NULL);
		EXECUTE 'COPY temp_verter FROM ''' || p_file_path || ''' WITH NULL AS ''None'' delimiter ''' || p_file_delim || ''' CSV HEADER;';
		INSERT INTO verter
		SELECT id, "Check", (enum_range(NULL::status))[state_num+1], "Time"
		FROM temp_verter;
		DROP TABLE temp_verter;
	ELSE
		EXECUTE 'COPY ' || p_table_name || ' FROM ''' || p_file_path || ''' WITH NULL AS ''None'' delimiter ''' || p_file_delim || ''' CSV HEADER;';
	END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE export_to_csv(
    IN p_table_name TEXT,
    IN p_file_path TEXT,
	IN p_file_delim TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'COPY ' || p_table_name || ' TO ''' || p_file_path || ''' WITH delimiter ''' || p_file_delim || ''' CSV HEADER;';
END;
$$;

/********************************************************************************/
/*																				*/
/*				ПРОВЕРКА - ЗАГРУЗКА ПРЕДОСТАВЛЕННЫХ CSV-ФАЙЛОВ					*/
/*																				*/
/********************************************************************************/

/* на школьном компе */

CALL import_from_csv('peers','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/peers.csv',';');
CALL import_from_csv('tasks','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/tasks.csv',';');
CALL import_from_csv('checks','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/checks.csv',';');
CALL import_from_csv('p2p','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/p2p.csv',';');
CALL import_from_csv('Verter','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/verter.csv',';');
CALL import_from_csv('xp','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/xp.csv',';');
CALL import_from_csv('friends','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/friends.csv',';');
CALL import_from_csv('TableName_Recommendations','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/Recommendations.csv',';');
CALL import_from_csv('TableNameTimeTracking','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/time_tracking.csv',';');

/* на домашнем компе */

-- CALL import_from_csv('peers','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/peers.csv',';');
-- CALL import_from_csv('tasks','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/tasks.csv',';');
-- CALL import_from_csv('checks','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/checks.csv',';');
-- CALL import_from_csv('p2p','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/P2P.csv',';');
-- CALL import_from_csv('Verter','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/verter.csv',';');
-- CALL import_from_csv('xp','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/xp.csv',';');
-- CALL import_from_csv('friends','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/friends.csv',';');
-- CALL import_from_csv('TableName_Recommendations','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/recommendations.csv',';');
-- CALL import_from_csv('TableNameTimeTracking','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/time_tracking.csv',';');

/****************************************************************************/
/*																			*/
/* 				                	PART 4                 					*/
/*																			*/
/****************************************************************************/

------------------------ex01------------------------

CREATE OR REPLACE PROCEDURE destroys_TableName(
)
AS $$
DECLARE
    t_name RECORD;
BEGIN
    FOR t_name IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name ILIKE 'TableName%'
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(t_name.table_name) ||' CASCADE ';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/* Проверка */
SELECT table_name FROM information_schema.tables WHERE table_name ILIKE 'TableName%';

CALL destroys_TableName();

SELECT table_name FROM information_schema.tables WHERE table_name ILIKE 'TableName%';

------------------------ex02------------------------

CREATE OR REPLACE PROCEDURE scalar_functions(
	IN ref refcursor
	, OUT p_num_rows INT
)
AS $$
BEGIN
	/* создание временной таблицы из вывода команды \df */
	CREATE TEMP TABLE df_table_output ON COMMIT DROP AS
		SELECT n.nspname AS "Schema",
			p.proname AS "Name",
			pg_catalog.pg_get_function_result(p.oid) AS "Result data type",
			pg_catalog.pg_get_function_arguments(p.oid) AS "Argument data types",
			CASE
				WHEN p.prokind = 'a' THEN 'agg'
				WHEN p.prokind = 'w' THEN 'window'
				WHEN p.prokind = 'p' THEN 'proc'
				ELSE 'func'
			END AS "Type"
		FROM pg_catalog.pg_proc p
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE pg_catalog.pg_function_is_visible(p.oid)
			AND n.nspname <> 'pg_catalog'
			AND n.nspname <> 'information_schema'
		ORDER BY 1, 2, 4;

	/* создание временной целевой таблицы из таблицы df_table_output */
	CREATE TEMP TABLE target_table ON COMMIT DROP AS
	SELECT "Name"||' (parameters: '||"Argument data types"||')' AS "Scalar functions with parameters"
	FROM df_table_output
	WHERE "Argument data types" <> ''
	AND   "Name" IN (SELECT proname 
					 	FROM pg_catalog.pg_proc
					 	WHERE NOT proretset
					 	AND prokind <> 'p'
					   );

	/* count number of target_table rows */
	SELECT count(*) INTO p_num_rows FROM target_table;

	/* return of target_table */
	OPEN ref FOR
	SELECT * FROM target_table;
END;
$$ LANGUAGE plpgsql;

/* Проверка - начало */
BEGIN;
	DO $$
	DECLARE res INT;
	BEGIN
		CALL scalar_functions('fdsdfg', res);
		RAISE NOTICE 'num of rows = %', res;
	END $$;
	FETCH ALL IN "fdsdfg";
COMMIT;

/* специально созданные функции: скалярные и не скалярные с параметрами и без */

CREATE OR REPLACE FUNCTION scalar_func_1(
	IN p_table_name VARCHAR
) RETURNS INT
AS $$
DECLARE result INT;
BEGIN
	SELECT COUNT(*) FROM p_table_name INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scalar_func_2(
) RETURNS INT
AS $$
DECLARE result INT;
BEGIN
	SELECT COUNT(*) FROM peers INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scalar_func_3(
	IN p_id INT
) RETURNS DATE
AS $$
DECLARE result DATE;
BEGIN
	SELECT "Date" FROM checks WHERE id = p_id INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nonscalar_func_1(
) RETURNS TABLE (LIKE tasks)
AS $$
BEGIN
	RETURN QUERY SELECT * FROM tasks limit 15;
END;
$$ LANGUAGE plpgsql;

/* Проверка - конец */
BEGIN;
	DO $$
	DECLARE res INT;
	BEGIN
		CALL scalar_functions('fdsdfg', res);
		RAISE NOTICE 'num of rows = %', res;
	END $$;
	FETCH ALL IN "fdsdfg";
COMMIT;

------------------------ex03------------------------

CREATE OR REPLACE PROCEDURE destroys_triggers(
	OUT p_num INT
)
AS $$
DECLARE
	t_name RECORD;
    num INT := 0;
BEGIN
    FOR t_name IN
        SELECT DISTINCT trigger_name, event_object_table
        FROM information_schema.triggers
    LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(t_name.trigger_name) || ' ON ' || quote_ident(t_name.event_object_table);
		num := num + 1;
    END LOOP;
	p_num := num;
END;
$$ LANGUAGE plpgsql;

/* Проверка */
SELECT DISTINCT trigger_name, event_object_table FROM information_schema.triggers;

DO $$
DECLARE res INT;
BEGIN
	CALL destroys_triggers(res);
	RAISE NOTICE 'num of rows = %', res;
END $$;

SELECT DISTINCT trigger_name, event_object_table FROM information_schema.triggers;

------------------------ex04------------------------

CREATE OR REPLACE PROCEDURE funcs_by_string(
	IN ref refcursor
	, IN p_string VARCHAR
)
AS $$
BEGIN
	/* создание временной целевой таблицы из вывода команды \df+ */
	CREATE TEMP TABLE target_table ON COMMIT DROP AS
		SELECT p.proname AS "Name",
			CASE
				WHEN p.prokind = 'a' THEN 'agg'
				WHEN p.prokind = 'w' THEN 'window'
				WHEN p.prokind = 'p' THEN 'proc'
				ELSE 'func'
			END AS "Type"
		FROM pg_catalog.pg_proc p
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE pg_catalog.pg_function_is_visible(p.oid)
			AND n.nspname <> 'pg_catalog'
			AND n.nspname <> 'information_schema'
			AND NOT proretset
			AND p.prosrc LIKE '%'||p_string||'%'
		ORDER BY 1;

	/* return of target_table */
	OPEN ref FOR
	SELECT * FROM target_table;
END;
$$ LANGUAGE plpgsql;

/* Проверка - начало */
BEGIN;
	DO $$
	DECLARE input VARCHAR := 'tus))[state_num+1], "Time"';
	BEGIN
		CALL funcs_by_string('fdsdfg', input);
	END $$;
	FETCH ALL IN "fdsdfg";
COMMIT;

------------------------ex02------------------------
----------------------version1----------------------
/* using OUT parameter and return list through RAISE NOTICE in LOOP */
-- CREATE OR REPLACE PROCEDURE list_of_functions(
-- 	OUT p_num_rows INT
-- )
-- AS $$
-- DECLARE p_res RECORD;
-- BEGIN
-- 	p_num_rows := 0;
-- 	FOR p_res IN
-- 		SELECT * FROM tasks limit 15
-- 	LOOP
-- 		RAISE NOTICE '%', p_res;
-- 		p_num_rows := p_num_rows + 1;
-- 	END LOOP;

-- END;
-- $$ LANGUAGE plpgsql;

/* Проверка 1 */
-- DO $$
-- DECLARE res INT := 0;
-- BEGIN
-- 	CALL list_of_functions(res);
--     RAISE NOTICE 'num of rows = %', res;
-- END $$;

----------------------version2----------------------
/* using refcursor without OUT parameter
   to test table return FROM procedure in DO block */
-- CREATE OR REPLACE PROCEDURE list_of_functions_2(
-- 	IN qwe refcursor
-- )
-- AS $$
-- BEGIN

-- 	OPEN qwe FOR
-- 	SELECT *
-- 	FROM Tasks limit 15;

-- END;
-- $$ LANGUAGE plpgsql;

/* Проверка 2 */
-- BEGIN;
-- 	CALL list_of_functions_2('fdsdfg');
-- 	FETCH ALL IN "fdsdfg";
-- COMMIT;

-- DO $$
-- 	BEGIN
-- 	CALL list_of_functions_2('fdsdfg');
-- END $$;

-- BEGIN;
-- 	DO $$
-- 		BEGIN
-- 		CALL list_of_functions_2('fdsdfg');
-- 	END $$;
-- 	FETCH ALL IN "fdsdfg";
-- COMMIT;

----------------------version3----------------------
/* using refcursor and OUT parameter */
-- CREATE OR REPLACE PROCEDURE list_of_functions_3(
-- 	IN ref refcursor
-- 	, OUT p_num_rows INT
-- )
-- AS $$
-- BEGIN
-- 	CREATE TEMP TABLE target_table ON COMMIT DROP AS
-- 	/* example of to be created target_table */
-- 	SELECT * FROM tasks;
-- 	/* count number of target_table rows */
-- 	SELECT count(*) INTO p_num_rows FROM target_table;
-- 	/* return of target_table */
-- 	OPEN ref FOR
-- 	SELECT * FROM target_table limit 15;
-- END;
-- $$ LANGUAGE plpgsql;

/* Проверка 3 */
-- DO $$
-- DECLARE res INT;
-- BEGIN
-- 	CALL list_of_functions_3('fdsdfg',res);
--     RAISE NOTICE 'num of rows = %', res;
-- END $$;

-- BEGIN;
-- 	DO $$
-- 	DECLARE res INT;
-- 	BEGIN
-- 		CALL list_of_functions_3('fdsdfg', res);
-- 		RAISE NOTICE 'num of rows = %', res;
-- 	END $$;
-- 	FETCH ALL IN "fdsdfg";
-- COMMIT;

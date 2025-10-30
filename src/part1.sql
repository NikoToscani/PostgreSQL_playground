/****************************************************************************/
/*																			*/
/*						УДАЛЕНИЕ ОБЪЕКТОВ БД ИЗ PART1						*/
/*																			*/
/****************************************************************************/

/* Триггер вызывает функию, которая проверяет наличие нуливого ParentTask / root task,
RAISE EXCEPTION прерывает выполнение операции */
DROP TRIGGER IF EXISTS trg_Tasks ON Tasks;
/* Триггер вызывает функцию, которая просто проверяет несоответствия при записи и выдает ошибку,
препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
DROP TRIGGER IF EXISTS trg_p2p ON P2P;
/* Триггер вызывает функцию, которая проверяет несоответствия при записи и выдает ошибку,
препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
DROP TRIGGER IF EXISTS trg_verter ON Verter;
/* активирует функцию трансфера пир-поинтов между пирами после внесения записи в P2P */
DROP TRIGGER IF EXISTS trg_p2p_update_points ON P2P;

DROP TABLE IF EXISTS Peers CASCADE;
DROP TABLE IF EXISTS Verter CASCADE;
DROP TABLE IF EXISTS Tasks CASCADE;
DROP TABLE IF EXISTS Friends CASCADE;
DROP TABLE IF EXISTS Checks CASCADE;
DROP TABLE IF EXISTS TransferredPoints CASCADE;
DROP TABLE IF EXISTS P2P CASCADE;
DROP TABLE IF EXISTS XP CASCADE;
DROP TABLE IF EXISTS TimeTracking CASCADE;
DROP TABLE IF EXISTS Recommendations CASCADE;
-- /*временные таблицы для передачи данных из файла P2P.csv в таблицу P2P*/
-- DROP TABLE IF EXISTS temp_P2P;
-- /*временные таблицы для передачи данных из файла verter.csv в таблицу verter*/
-- DROP TABLE IF EXISTS temp_verter;

/* Функия проверяет наличие нуливого ParentTask / root task, 
RAISE EXCEPTION прерывает выполнение операции в случае ошибки */
DROP FUNCTION IF EXISTS single_root();
/* Функция проверяет несоответствия при записи:
- если новая запись имеет статус Старт, а в таблице уже есть записмь со статусом Старт
- если новая запись имеет статус 'Success', 'Failure' и время новой записи меньше или равно
времени для записи с таким же Чек.ИД (окончание проверки не модет быть раньше начала)
- а также производит поиск по имеющимся записям на предмет наличия записи со статустом Старт 
для такого же Чек.ИД
и выдает ошибку,препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
DROP FUNCTION IF EXISTS only_one_start_p2p();
/* Функция просто проверяет несоответствия при записи:
- если новая запись имеет статус 'Старт', а в таблице уже есть записмь со статусом 'Старт'
- если новая запись имеет статус 'Success', 'Failure' и время новой записи меньше или равно
времени для записи с таким же Чек.ИД (окончание проверки не модет быть раньше начала)
- а также производит поиск по имеющимся записям на предмет наличия записи со статустом Старт 
для такого же Чек.ИД
и выдает ошибку, препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
DROP FUNCTION IF EXISTS only_one_start_verter();
/* функция трансфера пир-поинтов между пирами после внесения записи в P2P */
DROP FUNCTION IF EXISTS trg_p2p_update_points();

DROP PROCEDURE IF EXISTS import_from_csv(TEXT,TEXT,TEXT);
DROP PROCEDURE IF EXISTS export_to_csv(TEXT,TEXT,TEXT);

DROP TYPE IF EXISTS status CASCADE;

/****************************************************************************/
/*																			*/
/*						УДАЛЕНИЕ ОБЪЕКТОВ БД ИЗ PART2						*/
/*																			*/
/****************************************************************************/

-- DROP TRIGGER IF EXISTS trg_p2p_update_points ON P2P;
-- DROP TRIGGER IF EXISTS trg_valid_xp ON XP;

-- DROP PROCEDURE IF EXISTS add_p2p_check(
--     IN p_check_peer VARCHAR,
--     IN p_checker_peer VARCHAR,
--     IN p_task_name VARCHAR,
--     IN p_state status,
--     IN p_time TIME
-- );

-- DROP PROCEDURE IF EXISTS add_verter_check(
--     IN p_nickname VARCHAR,
--     IN p_task_name VARCHAR,
--     "state" status,
--     IN p_time TIME
-- );

-- DROP FUNCTION IF EXISTS p2p_update_points();
-- DROP FUNCTION IF EXISTS valid_xp();

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

CREATE TABLE IF NOT EXISTS TimeTracking (
  ID SERIAL PRIMARY KEY,
  Peer VARCHAR NOT NULL,
  "Date" DATE NOT NULL,
  "Time" TIME WITHOUT TIME ZONE NOT NULL,
  "State" VARCHAR NOT NULL,
  CONSTRAINT valid_state CHECK ("State" IN ('1', '2')),
  CONSTRAINT fk_peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Recommendations (
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

/* Функия проверяет наличие нуливого ParentTask / root task, 
RAISE EXCEPTION прерывает выполнение операции в случае ошибки */
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

/* Триггер вызывает функию, которая проверяет наличие нуливого ParentTask / root task,
RAISE EXCEPTION прерывает выполнение операции */
CREATE OR REPLACE TRIGGER trg_Tasks -- срабатывает при новой записи в Tasks
BEFORE INSERT OR UPDATE ON Tasks
FOR EACH ROW
EXECUTE FUNCTION single_root();

/* Функция проверяет несоответствия при записи:
- если новая запись имеет статус Старт, а в таблице уже есть запись со статусом Старт
- если новая запись имеет статус 'Success', 'Failure' и время новой записи меньше или равно
времени для записи с таким же Чек.ИД (окончание проверки не модет быть раньше начала)
- а также производит поиск по имеющимся записям на предмет наличия записи со статустом Старт 
для такого же Чек.ИД
и выдает ошибку,препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
CREATE OR REPLACE FUNCTION only_one_start_p2p()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW."State" = 'Start' THEN
		IF EXISTS (
			SELECT * 
			FROM P2P
			WHERE id = NEW.id AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check already has Start status';
		END IF;
	END IF;
	IF NEW."State" IN ('Success', 'Failure') THEN
		IF NEW."Time" < (SELECT "Time"
					   FROM P2P
					   WHERE NEW."Check" = P2P."Check"
					   ORDER BY "Time" DESC
					   LIMIT 1) THEN
		RAISE EXCEPTION 'Check can not be finished earlier then it''''s start';
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

/* Триггер вызывает функцию, которая проверяет несоответствия при записи и выдает ошибку,
препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
CREATE OR REPLACE TRIGGER trg_p2p
BEFORE INSERT OR UPDATE ON P2P
FOR EACH ROW
EXECUTE FUNCTION only_one_start_p2p();


/* Функция просто проверяет несоответствия при записи:
- если новая запись имеет статус 'Старт', а в таблице уже есть запись со статусом 'Старт'
- если новая запись имеет статус 'Success', 'Failure' и время новой записи меньше или равно
времени для записи с таким же Чек.ИД (окончание проверки не модет быть раньше начала)
- а также производит поиск по имеющимся записям на предмет наличия записи со статустом Старт 
для такого же Чек.ИД
и выдает ошибку, препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
CREATE OR REPLACE FUNCTION only_one_start_verter()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW."State" = 'Start' THEN
		IF EXISTS (
			SELECT * 
			FROM Verter
			WHERE id = NEW.id AND "State" = 'Start'
		)
		THEN RAISE EXCEPTION 'Check already has Start status';
		END IF;
	END IF;
	IF NEW."State" IN ('Success', 'Failure') THEN
		IF NEW."Time" < (SELECT "Time"
					   FROM Verter
					   WHERE NEW."Check" = Verter."Check"
					   ORDER BY "Time" DESC
					   LIMIT 1) THEN
		RAISE EXCEPTION 'Check can not be finished earlier then it''''s start';
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

/* Триггер вызывает функцию, которая проверяет несоответствия при записи и выдает ошибку,
препятствуя внесению неправильной записи в таблицу (RAISE EXCEPTION прерывает выполнение операции) */
CREATE OR REPLACE TRIGGER trg_verter
BEFORE INSERT OR UPDATE ON Verter
FOR EACH ROW
EXECUTE FUNCTION only_one_start_verter();


/* функция трансфера пир-поинтов между пирами после внесения записи в P2P */
CREATE OR REPLACE FUNCTION trg_p2p_update_points()
RETURNS TRIGGER AS $$
DECLARE
  peer_value VARCHAR;
BEGIN
  IF NEW."State" IN ('Success', 'Failure') THEN
	/* Блок BEGIN - END ниже мне не понятен.
	   Мне кажется, изначально таблица Checks должна быть пуста, и заполняться после внесения записи в P2P и в Verter.
	   Таким образом, зачем проверять, есть ли в этой таблице имя проверяемого Пира для данной записи Чек.ИД?
	   С другой стороны, если мы сначала знаем все записи в таблице Чек, а после заполняем P2P и Verter, тогда
	   есть смысл проверять, но это нарушает логику процесса.
	   Допустим второй вариант, и запись в Check генерируется, когда ожидаемый проверку Пир для Task нажал кнопку Сабмит.
	   При этом дата проверки должна обновиться при завершении проверки P2P и, при необходимости проверки Вертером
	   должна генерироваться и вноситься еще одна запись проверки. */
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
	/* Есть только увеличение пир-поинтов для проверяющего Пира, но нет уменьшения для проверяемого.
	Н так как нигде нет счета для каждого Пира, то в рамках задачи нет такой задачи как уменьшение  */
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
  			/* Нет, возвращаемое значение для строчного триггера AFTER всегда игнорируется */
END;
$$ LANGUAGE plpgsql;

/* активирует функцию трансфера пир-поинтов между пирами после внесения записи в P2P */
CREATE OR REPLACE TRIGGER trg_p2p_update_points
AFTER INSERT ON P2P
FOR EACH ROW
EXECUTE FUNCTION trg_p2p_update_points();

/****************************************************************************/
/*																			*/
/* 					ПРОЦЕДУРЫ ЗАГРУЗКИ/ВЫГРУЗКИ CSV-ФАЙЛОВ 					*/
/*																			*/
/****************************************************************************/

----------------import/export----------------

-- нужно для корректной подгрузки дат
SET datestyle = dmy;

CREATE OR REPLACE PROCEDURE import_from_csv(
    IN p_table_name TEXT,
    IN p_file_path TEXT,
	IN p_file_delim TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
	/* копирование в таблицу P2P через временную таблицу */
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
	/* копирование в таблицу VERTER через временную таблицу */
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

-- call import_from_csv('peers','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/peers.csv',';');
-- call import_from_csv('tasks','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/tasks.csv',';');
-- call import_from_csv('checks','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/checks.csv',';');
-- call import_from_csv('p2p','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/p2p.csv',';');
-- call import_from_csv('Verter','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/verter.csv',';');
-- call import_from_csv('xp','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/xp.csv',';');
-- call import_from_csv('friends','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/friends.csv',';');
-- call import_from_csv('Recommendations','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/Recommendations.csv',';');
-- call import_from_csv('TimeTracking','/Users/monetkar/sql/SQL2_Info21_v1.0-1/src/time_tracking.csv',';');

/* на домашнем компе */

call import_from_csv('peers','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/peers.csv',';');
call import_from_csv('tasks','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/tasks.csv',';');
call import_from_csv('checks','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/checks.csv',';');
call import_from_csv('p2p','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/P2P.csv',';');
call import_from_csv('Verter','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/verter.csv',';');
call import_from_csv('xp','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/xp.csv',';');
call import_from_csv('friends','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/friends.csv',';');
call import_from_csv('Recommendations','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/recommendations.csv',';');
call import_from_csv('TimeTracking','/home/nikolai/21school/sql_training/SQL2_Info21_v1.0-1/src/time_tracking.csv',';');

BEGIN;
\ir part2.sql
END;

-- select * from peers limit 5;
-- select * from tasks limit 5;
-- select * from checks limit 5;
-- select * from p2p limit 5;
-- select * from transferredpoints ORDER BY id limit 5;
-- select * from verter limit 5;
-- select * from xp limit 5;
-- select * from friends limit 5;
-- select * from Recommendations limit 5;
-- select * from timetracking limit 5;






-- /* активирует функцию перед внесением записи в таблицу ТаймТрекинг */
-- DROP TRIGGER IF EXISTS trg_timetracking ON TimeTracking;

-- /* Функция вызывается перед внесением записи в таблицу ТаймТреккинг 
--    Проверяет и прерывает запись, если обнаруживаются несоответствия:
--    - вносимая Дата или Время меньше / меньше или равно самых последних Даты и Времени, внесенных в таблицу для данного Пира 
--    - вносимый Статус равен последнему внесенному Статусу для данного Пира 
--    - если у новой записи статус 2 (out) и дата последней записи не равна дате новой записи
--      добавить соответствующие записи с выходом-входом в полночь */
-- DROP FUNCTION IF EXISTS insert_new_state();


-- /* Функция вызывается перед внесением записи в таблицу ТаймТреккинг 
--    Проверяет и прерывает запись, если обнаруживаются несоответствия:
--    - вносимая Дата или Время меньше / меньше или равно самых последних Даты и Времени, внесенных в таблицу для данного Пира 
--    - вносимый Статус равен последнему внесенному Статусу для данного Пира 
--    - если у новой записи статус 2 (out) и дата последней записи не равна дате новой записи
--      добавить соответствующие записи с выходом-входом в полночь */
-- CREATE OR REPLACE FUNCTION insert_new_state()
-- RETURNS TRIGGER AS $$
-- 	DECLARE last_state VARCHAR(1);
-- 	DECLARE last_time TIME WITHOUT TIME ZONE;
-- 	DECLARE last_date DATE;
-- BEGIN
-- 	 WITH last_record AS(
-- 	 	 SELECT "State", "Time", "Date"
-- 		 FROM TimeTracking
-- 		 WHERE Peer = New.Peer
-- 		 ORDER BY "Date" DESC, "Time" DESC
-- 		 LIMIT 1
-- 	 )
-- 	 SELECT "State", "Time", "Date" INTO last_state, last_time, last_date
-- 	 FROM last_record;

-- 	 IF (NEW."Date" < last_date) OR (NEW."Date" = last_date AND NEW."Time" <= last_time) THEN
-- 	  	RAISE EXCEPTION 'New time/date cannot be earlier than the last state';
-- 	 END IF;

-- 	 IF NEW."State" = last_state THEN
-- 	  	RAISE EXCEPTION 'New state cannot be the same as the previous one';
-- 	 END IF;

-- 	 IF NEW."State" = '2' AND last_date != NEW."Date" THEN
-- 	  	INSERT INTO TimeTracking (Peer, "Date", "Time", "State")
-- 	  	VALUES (NEW.Peer, last_date, TIME '23:59:59', '2'),
-- 			 (NEW.Peer, NEW."Date", TIME '00:00:00', '1');
-- 	 END IF;

-- 	 RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;


-- /* активирует функцию перед внесением записи в таблицу ТаймТрекинг */
-- CREATE OR REPLACE TRIGGER trg_timetracking
-- BEFORE INSERT ON TimeTracking
-- FOR EACH ROW
-- EXECUTE FUNCTION insert_new_state();

------------------------ex01------------------------
CREATE OR REPLACE PROCEDURE add_p2p_check(
    IN p_checked_peer VARCHAR,
    IN p_checking_peer VARCHAR,
    IN p_task_name VARCHAR,
    IN p_state status,
    IN p_time TIME
)
LANGUAGE PLPGSQL
AS $$
BEGIN
    IF p_state = 'Start' THEN
        INSERT INTO checks(id, peer, task, "Date")
        VALUES((SELECT MAX(ID)+1 FROM checks), p_checked_peer, p_task_name, CURRENT_DATE);
        INSERT INTO P2P(id, "Check", checkingpeer, "State", "Time")
        VALUES ((SELECT MAX(ID)+1 FROM P2P), (SELECT MAX(ID) FROM checks), p_checking_peer, p_state, p_time);
    ELSE
        INSERT INTO P2P(id, "Check", checkingpeer, "State", "Time")
        VALUES ((SELECT MAX(ID)+1 FROM P2P),
                (SELECT checks.id
                 FROM checks
                 WHERE checks.peer = p_checked_peer
                 AND checks.task = p_task_name
                 ORDER BY checks.id DESC
                 LIMIT 1),
                p_checking_peer, p_state, p_time);
    END IF;
END;
$$;

SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM p2p ORDER BY id DESC LIMIT 6;
/* проверка что соотвтетсвующие корректные checks.id присваиваются соответствующим p2p."check" */
CALL add_p2p_check('cpwfiewvim', 'ptznpzekkj', 'C7', 'Start', '15:30:00');
CALL add_p2p_check('kdvfscrdbf', 'vrbyeonaxg', 'C7', 'Start', '15:31:00');
CALL add_p2p_check('cpwfiewvim', 'ptznpzekkj', 'C7', 'Failure', '15:54:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM p2p ORDER BY id DESC LIMIT 6;
CALL add_p2p_check('kdvfscrdbf', 'vrbyeonaxg', 'C7', 'Success', '16:05:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM p2p ORDER BY id DESC LIMIT 6;
/* проверка что нельзя внести запись со статусом Success или Failure в p2p,
   если ранее у нее не было записи Start */
CALL add_p2p_check('iodskxuoka', 'sdgkxkjwmk', 'C7', 'Success', '16:30:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM p2p ORDER BY id DESC LIMIT 6;

----------------------ex02------------------------
CREATE OR REPLACE PROCEDURE add_verter_check(
    IN p_nickname VARCHAR,
    IN p_task_name VARCHAR,
    "state" status,
    IN p_time TIME
) 
LANGUAGE PLPGSQL
AS $$
BEGIN
    WITH latest_p2p_step AS (
        SELECT ch.ID AS check_id
        FROM P2P AS pp
            LEFT JOIN Checks AS ch
                ON (ch.ID = pp."Check")
        WHERE (pp."Time" < p_time) AND (pp."State" = 'Success') AND (pp."Check" IS NOT NULL)
        ORDER BY pp."Time" DESC
        LIMIT 1
    )
    INSERT INTO Verter (id, "Check", "State", "Time")
    SELECT
        (SELECT MAX(ID)+1 FROM Verter),
        (SELECT checks.id
         FROM checks
         WHERE checks.peer = p_nickname
         AND checks.task = p_task_name
         ORDER BY checks.id DESC
         LIMIT 1),
        "state", p_time
    WHERE EXISTS (SELECT 1 FROM latest_p2p_step);
END;
$$;

SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM verter ORDER BY id DESC LIMIT 6;
/* проверка что соотвтетсвующие корректные checks.id присваиваются соответствующим verter."check" */
CALL add_verter_check('cpwfiewvim', 'C7', 'Start', '16:30:00');
CALL add_verter_check('kdvfscrdbf', 'C7', 'Start', '16:31:00');
CALL add_verter_check('cpwfiewvim', 'C7', 'Failure', '16:54:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM verter ORDER BY id DESC LIMIT 6;
CALL add_verter_check('kdvfscrdbf', 'C7', 'Success', '17:05:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM verter ORDER BY id DESC LIMIT 6;
/* проверка что нельзя внести запись со статусом Success или Failure в p2p,
   если ранее у нее не было записи Start */
CALL add_verter_check('iodskxuoka', 'C7', 'Success', '17:30:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 6;
SELECT * FROM verter ORDER BY id DESC LIMIT 6;

------------------------ex03------------------------
CREATE OR REPLACE FUNCTION p2p_update_points()
RETURNS TRIGGER AS $$
DECLARE
  peer_value VARCHAR;
BEGIN
  SELECT Peer INTO peer_value
  FROM Checks
  WHERE Checks.ID = NEW."Check"
  LIMIT 1;
  IF NEW."State" IN ('Success', 'Failure') THEN
    	UPDATE TransferredPoints
    	SET PointsAmount = PointsAmount + 1
    	WHERE NEW.CheckingPeer = TransferredPoints.CheckingPeer 
		AND peer_value = TransferredPoints.CheckedPeer;
	ELSE 
		IF NOT EXISTS (
			SELECT *
			FROM TransferredPoints
			WHERE NEW.CheckingPeer = TransferredPoints.CheckingPeer 
			AND peer_value = TransferredPoints.CheckedPeer
		) THEN
		INSERT INTO TransferredPoints(CheckingPeer, CheckedPeer)
		VALUES(NEW.CheckingPeer, (SELECT Peer
							FROM Checks
							WHERE Checks.ID = NEW."Check"
							LIMIT 1));
		END IF;
	END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_p2p_update_points
AFTER INSERT ON P2P
FOR EACH ROW
EXECUTE FUNCTION p2p_update_points();

SELECT * FROM checks ORDER BY id DESC LIMIT 3;
SELECT * FROM p2p ORDER BY id DESC LIMIT 3;
SELECT * FROM TransferredPoints ORDER BY id DESC LIMIT 3;
/* должны появиться
   - новая запись check
   - новая запись p2p со статусом Start
   - новая запись transferpoint со значением 0 */
CALL add_p2p_check('iodskxuoka', 'sdgkxkjwmk', 'C7', 'Start', '16:30:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 3;
SELECT * FROM p2p ORDER BY id DESC LIMIT 3;
SELECT * FROM TransferredPoints ORDER BY id DESC LIMIT 3;
/* должны появиться
   - новая запись  со статусом Failure
   - новая запись transferpoint со значением 1 */
CALL add_p2p_check('iodskxuoka', 'sdgkxkjwmk', 'C7', 'Failure', '16:40:00');
SELECT * FROM checks ORDER BY id DESC LIMIT 3;
SELECT * FROM p2p ORDER BY id DESC LIMIT 3;
SELECT * FROM TransferredPoints ORDER BY id DESC LIMIT 3;

------------------------ex04------------------------
CREATE OR REPLACE FUNCTION valid_xp()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.XPAmount > (SELECT Tasks.MaxXP
					   FROM Tasks
					   JOIN Checks ON Tasks.Title = Checks.Task
					   WHERE NEW."Check" = Checks.id)
	THEN RAISE EXCEPTION 'XP amount can''''t exceed maximum XP for current task';			
	END IF;
	IF NOT EXISTS (
        SELECT *
        FROM P2P
        LEFT JOIN verter ON P2P."Check" = verter."Check"
        WHERE P2P."Check" = NEW."Check" 
        AND P2P."State" = 'Success'
        AND (verter."State" = 'Success' OR Verter."State" IS NULL)
    )
	THEN RAISE EXCEPTION 'Check id = % does not have Success status on P2P or on Verter check', NEW."Check";
	END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_valid_xp
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION valid_xp();

/* проверка с проектом без вертера */
SELECT * FROM tasks WHERE title = 'SQL3';
SELECT * FROM checks WHERE task = 'SQL3' LIMIT 3;
SELECT * FROM xp ORDER BY id DESC LIMIT 3;
/* попытка внести проект с Failure */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 4790, 100);
SELECT * FROM xp ORDER BY id DESC LIMIT 4;
/* попытка внести XP больше допустимого максимума */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 11233, 100500);
SELECT * FROM xp ORDER BY id DESC LIMIT 4;
/* внесение записи о проекте со всеми удовлетварительными критериями */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 11233, 160);
SELECT * FROM xp ORDER BY id DESC LIMIT 4;

/* проверка с проектом с вертером */
SELECT * FROM tasks WHERE title = 'C2';
SELECT * FROM checks WHERE task = 'C2' LIMIT 5;
SELECT * FROM xp ORDER BY id DESC LIMIT 5;
/* попытка внести проект с Failure на P2P */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 20, 260);
SELECT * FROM xp ORDER BY id DESC LIMIT 5;
/* попытка внести проект с Success на P2P и Failure на P2P */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 39, 260);
SELECT * FROM xp ORDER BY id DESC LIMIT 5;
/* попытка внести XP больше допустимого максимума */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 229, 900);
SELECT * FROM xp ORDER BY id DESC LIMIT 5;
/* внесение записи о проекте со всеми удовлетварительными критериями */
INSERT INTO xp (id, "Check", xpamount) VALUES((SELECT MAX(ID)+1 FROM xp), 229, 260);
SELECT * FROM xp ORDER BY id DESC LIMIT 5;

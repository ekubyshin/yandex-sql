-- задание 1

CREATE OR REPLACE PROCEDURE update_employees_rate(in p_emp json)
LANGUAGE plpgsql
    AS $$
	DECLARE _r json;
	BEGIN
		FOR _r IN SELECT 
			*
			FROM
				json_array_elements(p_emp)
		LOOP
			UPDATE employees as e
			SET rate = e.rate + (e.rate * (_r ->> 'rate_change')::INT) / 100
			WHERE e.id = (_r ->> 'employee_id')::UUID;
		END LOOP;
	END;
    $$;


-- задание 2

CREATE OR REPLACE PROCEDURE indexing_salary(in p_rate int)
LANGUAGE plpgsql
    AS $$
	DECLARE _avg NUMERIC;
	BEGIN
		select avg(e.rate) into _avg from employees e;
		UPDATE employees as e
		SET rate = case
			when rate < _avg then rate + rate * (p_rate + 2) / 100
			else rate + rate * p_rate / 100
		end;
	END;
    $$;


-- задание 3


CREATE OR REPLACE PROCEDURE close_project(in p_id UUID)
LANGUAGE plpgsql
    AS $$
	DECLARE _load record;
		_diff BIGINT;
		_bonus BIGINT;
		_cur_p record;
		_e UUID;
	BEGIN
		
			SELECT * FROM projects INTO _cur_p WHERE id = p_id;
			IF _cur_p.is_active = false THEN
				RAISE EXCEPTION 'Project already closed'; RETURN;
			END IF;
			UPDATE projects 
				SET is_active = false 
				WHERE ID = p_id;
			SELECT 
				SUM(work_hours) sum_hours,
				COUNT(employee_id) AS workers_count
				INTO _load 
				FROM logs
				WHERE project_id = p_id;
			
			IF _cur_p.estimated_time IS NOT NULL THEN _diff := _cur_p.estimated_time - _load.sum_hours;
			ELSE RETURN; END IF;
			_bonus := FLOOR(_diff * 0.75 / _load.workers_count);
			IF _bonus > 16 THEN _bonus := 16; END IF;
			IF _bonus > 0 THEN
				INSERT INTO logs(employee_id, project_id, work_date, work_hours)
				SELECT 
					DISTINCT l.employee_id, 
					p_id, 
					current_date, 
					_bonus  
				FROM logs l 
				WHERE l.project_id = p_id;
			END IF;
	END;
    $$;


-- Задание 4

CREATE OR REPLACE PROCEDURE log_work(in p_emp uuid, in p_proj uuid, in p_date date, in p_hours int)
LANGUAGE plpgsql
    AS $$
	DECLARE _cur_p record;
	_required_review BOOLEAN := false;
	BEGIN
		SELECT * INTO _cur_p FROM projects WHERE id = p_proj;
        IF _cur_p.is_active = false THEN RAISE EXCEPTION 'Project already closed'; RETURN; END IF;
		IF p_hours < 1 OR p_hours > 24 THEN RAISE EXCEPTION 'Invalid hours'; RETURN; END IF;
		IF p_hours > 16 THEN _required_review := true; END IF;
		IF p_date > current_date THEN _required_review := true; END IF;
		IF p_date < current_date - 7 THEN _required_review := true; END IF;
		INSERT INTO logs(employee_id, project_id, work_date, work_hours, required_review)
			VALUES (p_emp, p_proj, p_date, p_hours, _required_review);
	END;
    $$;

-- Задание 5

CREATE TABLE IF NOT EXISTS employee_rate_history(
	id SERIAL PRIMARY KEY,
	employee_id UUID NOT NULL,
	rate INT NOT NULL DEFAULT 0,
	from_date date NOT NULL DEFAULT CURRENT_DATE,
	CONSTRAINT fk_emp FOREIGN KEY (employee_id) REFERENCES employees(id)
);

INSERT INTO employee_rate_history(employee_id, rate, from_date)
SELECT 
	id AS employee_id, 
	rate AS rate,
	'2020-12-26'::DATE AS from_date
	FROM employees;

CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS trigger
LANGUAGE plpgsql
    AS $$
	BEGIN
		INSERT INTO employee_rate_history(employee_id, rate)
			VALUES (NEW.id, NEW.rate);
		RETURN NEW;
	END;
    $$
;
	
CREATE OR REPLACE TRIGGER change_employee_rate
AFTER INSERT OR UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();


-- Задание 6

CREATE OR REPLACE FUNCTION best_project_workers(in p_id uuid)
returns table(employee text, work_hours int)
LANGUAGE sql
    AS $$
		SELECT
			e.name AS employee,
			SUM(l.work_hours) AS work_hours
		FROM logs l
		JOIN employees e ON l.employee_id = e.id
		WHERE l.project_id = p_id
		GROUP BY e.name
		ORDER BY SUM(l.work_hours) DESC
		LIMIT 3;
    $$
;


-- Задание 7

CREATE OR REPLACE FUNCTION calculate_month_salary(in p_begin date, in p_end date)
RETURNS TABLE(id UUID, worked_hours int, salary numeric(9, 2))
LANGUAGE sql
    AS $$
		WITH s AS (SELECT
			l.employee_id,
			SUM(l.work_hours) AS worked_hours
			FROM logs l
			WHERE l.required_review = false AND l.is_paid = false AND l.work_date BETWEEN p_begin AND p_end
			GROUP BY l.employee_id)
		SELECT
			s.employee_id AS ID,
			s.worked_hours AS worked_hours,
			(CASE
				WHEN s.worked_hours <= 160 THEN e.rate * s.worked_hours
				ELSE e.rate * (s.worked_hours - 160) * 1.25 + e.rate * 160
			END)::NUMERIC(9,2) AS salary
		FROM s
		JOIN employees e ON s.employee_id = e.id
    $$
;
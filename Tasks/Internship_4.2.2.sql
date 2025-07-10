/*
Вы — стажер в компании, где отсутствует система отслеживания изменений пользователей. Нужно:

Логировать все изменения пользователей (name, email, role),

Хранить аудит изменений в отдельной таблице,

Раз в день экспортировать только свежие изменения в CSV,

Автоматизировать экспорт с помощью pg_cron.

Техническое задание.

1. Создайте функцию логирования изменений по трем полям.

2. Создайте trigger на таблицу users.

3. Установите расширение pg_cron.

4. Создайте функцию, которая будет доставать только свежие данные (за сегодняшний день) и будет сохранять их в образе Docker по пути /tmp/users_audit_export_, а далее указываете ту дату, за который этот csv был создан.

5. Установите планировщик pg_cron на 3:00 ночи.

 */



--Для начала создаем таблицы users и users_adult--

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

--Далее создадим триггерную функцию--

CREATE OR REPLACE FUNCTION audit_user_changes()
RETURNS TRIGGER AS $$
DECLARE
BEGIN
	IF NEW.name IS DISTINCT FROM OLD.name THEN
    	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    	VALUES (OLD.id, 'name', OLD.name, NEW.name, current_user);
	END IF;

	IF NEW.email IS DISTINCT FROM OLD.email THEN
    	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    	VALUES (OLD.id, 'email', OLD.email, NEW.email, current_user);
	END IF;

	IF NEW.role IS DISTINCT FROM OLD.role THEN
    	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    	VALUES (OLD.id, 'role', OLD.role, NEW.role, current_user);
	END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--А также сделаем сам триггер--

CREATE TRIGGER trigger_audit_user_changes
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION audit_user_changes();

--Активируем pg_cron--

CREATE EXTENSION IF NOT EXISTS pg_cron;

--Вставляем данные в таблицу users--

INSERT INTO users (name, email, role)
VALUES ('Alice', 'alice@example.com', 'users');

--Проверяем, что ланные появились в таблице--

SELECT * FROM users;

--Проверем содержимое таблицы users_audit--

SELECT * FROM users_audit;

--Чтобы данные появились в таблицу users_audit, нужно обновить данные в таблице user--

UPDATE users SET name = 'Alice Strange', email = 'alise.strange@example.com' WHERE id = 1; 

--Функция доставки свежих данных в csv-файл

CREATE OR REPLACE FUNCTION export_audit_to_csv() RETURNS void AS $outer$
DECLARE 
	PATH TEXT := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
BEGIN
	EXECUTE format(
		$inner$
		COPY (
			SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
			FROM users_audit
			WHERE changed_at >= NOW() - INTERVAL '1 day'
			ORDER BY changed_at
		) TO '%s' WITH CSV HEADER
		$inner$, path
	);
END;
$outer$ LANGUAGE plpgsql;

--Автоматизируем экспорт с помощью pg_cron--

SELECT cron.schedule(
	job_name := 'daily_audit_export',
	schedule := '0 3 * * *',
	command := $$SELECT export_audit_to_csv();$$
);

--Проверяем работоспособность cron--

SELECT * FROM cron.job;

--Выводим таблицу cron.job и смотрим в колонку 'active'. Должна стоять галочка--

SELECT export_audit_to_csv();
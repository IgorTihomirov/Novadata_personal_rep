/*Допустим, у Вас есть таблица users,
 и вы хотите автоматически сохранять историю
 изменений в таблицу users_history при каждом UPDATE.
 Вместо написания логики в приложении, можно использовать триггер.
 */

--Создадим таблицы users

 CREATE TABLE users (
	id SERIAL PRIMARY KEY,
	name TEXT,
	email TEXT
);

--И users_history

CREATE TABLE users_history (
	id SERIAL,
	user_id INT,
	old_name TEXT,
	old_email TEXT,
	changed_at TIMESTAMP DEFAULT now()
);

--Далее создадим триггерную функцию.

CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users_history(user_id, old_name, old_email)
    VALUES (OLD.id, OLD.name, OLD.email);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--А также сделаем сам триггер.

CREATE TRIGGER trigger_log_user_update
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_update();

/*Теперь каждый раз перед UPDATE в users будет срабатывать log_user_update,
и старые данные сохранятся в users_history.
 */

--Далее вставим данные в таблицу users. 

INSERT INTO public.users (name, email)
VALUES 
('Ivan Ivanov', 'ivan@example.com'),
('Anna Petrova', 'anna@example.com');

--И, как только мы обновим данные - сработает триггер. Сделаем это командой - 

UPDATE users
SET email = 'ivan.new@example.com'
WHERE name = 'Ivan Ivanov';

--И посмотрим, что за данные у нас теперь в обоих таблицах.

SELECT * FROM users_history;

SELECT * FROM users;

/*
Задача:

* У нас есть огромный поток логов: logs_raw

* Мы хотим каждый раз при вставке данных сразу
  агрегировать значения по минутам и пользователям.
  
* Потом — быстро строить отчёты по дням/часам.
*/

--Для начала создадим таблицу logs_raw:

-- 1. Основная "сырая" таблица
CREATE TABLE logs_raw (
    event_time DateTime,
    user_id UInt32,
    value UInt32
) ENGINE = MergeTree()
ORDER BY event_time;

--Далее, создаем агрегированную таблицу на основе движка AggregatingMergeTree.

CREATE TABLE logs_agg (
    event_minute DateTime,
    user_id UInt32,
    value_state AggregateFunction(sum, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_minute, user_id);

/*
Теперь создаем материализованное представление, которое будет срабатывать автоматически
и считать промежуточные состояния и записывать их в таблицу logs_agg.
*/

CREATE MATERIALIZED VIEW logs_mv
TO logs_agg
AS
SELECT 
    toStartOfMinute(event_time) AS event_minute,
    user_id,
    sumState(value) AS value_state
FROM logs_raw
GROUP BY event_minute, user_id;

/*
То есть на текущий момент 

* Clickhouse автоматически при каждой вставке в logs_raw агрегирует данные в logs_agg,
  не пересчитывая всё заново.
* Это значит, что мы делаем агрегацию сразу в sumState(...).

*/

--А сам запрос SELECT, который будет эффективен для аналитики будет выглядеть вот так

SELECT 
    toDate(event_minute) AS day,
    user_id,
    sumMerge(value_state) AS total_value
FROM logs_agg
GROUP BY day, user_id
ORDER BY day, user_id;

-- Заполним 1 миллион записей

INSERT INTO logs_raw
SELECT 
    now() - INTERVAL number SECOND,
    intHash32(number) % 1000,
    number % 10
FROM numbers(1000000);

--А далее сравним по скорости 2 запроса - 

-- Медленно: сканирует 1 миллион строк
SELECT sum(value) FROM logs_raw;



-- Быстро: агрегаты уже готовы
SELECT sumMerge(value_state) FROM logs_agg;

/*
Оба сработают моментально, но это вызвано простотой данных. 
При линейном увеличении их количества сразу можно заметить разницу. 
Для этого введите запрос для проверки времени запросов, только подождите 10-20 секунд,
чтобы они появились в списке:
*/

SELECT
    query,
    round(query_duration_ms / 1000.0, 3) AS seconds,
    read_rows,
    read_bytes
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 MINUTE
  AND type = 'QueryFinish'
  AND query ILIKE '%sum%'
ORDER BY event_time DESC
LIMIT 10;

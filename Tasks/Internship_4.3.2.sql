--Здесь рассматривается работа MV(Materialized View)--

--Создадим таблицу 'events'--

CREATE TABLE events (
    user_id UInt32,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time;

--А далее создадим таблицу аггрегации--

CREATE TABLE event_counts (
    event_type String,
    count UInt32
) ENGINE = SummingMergeTree()
ORDER BY event_type;

--И все, что остается сделать, это создать MV--

CREATE MATERIALIZED VIEW mv_event_counts
TO event_counts AS
SELECT
    event_type,
    count() AS count
FROM events
GROUP BY event_type;

/*Основная суть этого примера в том,
 что данные будут агрегироваться автоматически при каждой вставке данных в таблицу!*/

--Вставим данные--

INSERT INTO events VALUES (1, 'click', now()), (2, 'view', now()), (3, 'click', now());

/*В этом примере можно увидеть, что при создании таблиц использовались какие-то движки и,
вот именно, они являются особенностью Clickhouse, с помощью которых оптимизируют таблицы!*/

SELECT * FROM event_counts;


-- =============================================
-- 0. Подготовка данных
-- =============================================

-- Сбрасываем старую таблицу и создаем новую sales_data для загрузки CSV
DROP TABLE IF EXISTS sales_data;
CREATE TABLE sales_data (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID TEXT,
    Country TEXT
);

-- Импортируем данные из файла Online Retail.csv
COPY sales_data(
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country
)
FROM 'C:/VS_CODE_PROJECTS/PET_PROJECTS/SQL PROJECT/data/Online Retail.csv'
DELIMITER ';'
CSV HEADER;

-- =============================================
-- 1. Проверка данных после импорта
-- =============================================

-- Просмотр первых 10 строк
SELECT * 
FROM sales_data 
LIMIT 10;

-- Подсчет общего количества строк
SELECT COUNT(*) AS total_rows
FROM sales_data;

-- Проверка структуры таблицы (названий колонок и типов данных)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'sales_data';

-- =============================================
-- 2. Чистка данных
-- =============================================

-- Подсчет пропусков в ключевых колонках
SELECT
    COUNT(*) FILTER(WHERE description IS NULL) AS null_descriptions,
    COUNT(*) FILTER(WHERE customerid IS NULL) AS null_customers
FROM sales_data;

-- Поиск отрицательных или нулевых значений в quantity и unitprice
SELECT *
FROM sales_data
WHERE quantity <= 0 OR unitprice <= 0;

-- Удаление из данных:
-- 1) Возвраты (quantity <= 0), т.к. они не отражают покупательское поведение для RFM
-- 2) Строки без customerid (NULL), т.к. такие заказы невозможно отнести к клиенту

DELETE FROM sales_data
WHERE quantity <= 0 OR customerid IS NULL;

-- =============================================
-- 3. Добавление вычисляемых полей
-- =============================================

-- Добавление колонки total_price 
ALTER TABLE sales_data
ADD COLUMN total_price NUMERIC;

-- Подсчет суммы по каждой строке
UPDATE sales_data
SET total_price = quantity * unitprice;

-- =============================================
-- 4. Базовая аналитика
-- =============================================

-- 4.1 Топ стран по выручке
SELECT country, SUM(total_price) AS revenue
FROM sales_data
GROUP BY country
ORDER BY revenue DESC
LIMIT 10;

-- 4.2 Топ товаров по количеству продаж
SELECT description, SUM(quantity) AS total_sold
FROM sales_data
GROUP BY description
ORDER BY total_sold DESC
LIMIT 10;

-- 4.3 Топ клиентов по выручке
SELECT customerid, SUM(total_price) AS revenue
FROM sales_data
GROUP BY customerid
ORDER BY revenue DESC
LIMIT 10;

-- =============================================
-- 5. Анализ по времени
-- =============================================

-- Выручка по месяцам
SELECT 
    DATE_TRUNC('month', invoicedate) AS month,
    SUM(total_price) AS revenue
FROM sales_data
GROUP BY month
ORDER BY month;

-- Количество заказов по дням
SELECT 
    DATE(invoicedate) AS day,
    COUNT(DISTINCT invoiceno) AS orders
FROM sales_data
GROUP BY day
ORDER BY day;

-- =============================================
-- 6. RFM-анализ: расчет сырых метрик и присвоение баллов (1..5, 5 = лучше)
-- =============================================

-- 6.1 Сбор агрегатов на уровне клиента
-- Создание временной таблицы tmp_rfm_final для повторного использования RFM-метрик при анализе сегментов и топ-клиентов

CREATE TEMP TABLE temp_rfm_final AS 

-- Отбираем только тех клиентов, у которых суммарные покупки > 0
WITH rfm_base AS (
    SELECT 
        customerid,
        MAX(invoicedate)::date AS last_purchase,
        COUNT(DISTINCT invoiceno) AS frequency,
        SUM(total_price) AS monetary
    FROM sales_data
    GROUP BY customerid
    HAVING SUM(total_price) > 0
),

-- 6.2 Опорная дата для расчета recency (последний день в данных + 1 день)
reference_date AS (
    SELECT
        (MAX(last_purchase) + INTERVAL '1 day') AS today
    FROM rfm_base
),

-- 6.3 Расчет сырых метрик (recency в днях, frequency, monetary)
rfm_raw AS (
    SELECT
        rb.customerid,
        rb.last_purchase,
        EXTRACT(DAY FROM (rd.today - rb.last_purchase))::int AS recency,
        rb.frequency,
        rb.monetary
    FROM rfm_base AS rb
    CROSS JOIN reference_date AS rd
),

-- 6.4 Присвоение баллов 1..5 по RFM
-- Логика:
-- Recency: меньше дней = лучше → сортируем DESC (тогда "самые свежие" = 5)
-- Frequency и Monetary: больше = лучше → сортируем ASC (тогда "самые большие" = 5)

rfm_scores AS (
    SELECT 
        r.customerid,
        r.recency,
        r.frequency,
        r.monetary,
        NTILE(5) OVER(ORDER BY r.recency DESC) AS recency_score,
        NTILE(5) OVER(ORDER BY r.frequency ASC) AS frequency_score,
        NTILE(5) OVER (ORDER BY r.monetary ASC) AS monetary_score
    FROM rfm_raw AS r
)

-- 6.5 Создание текстового кода RFM (например: '5-3-4') и сегментов
    SELECT
        customerid,
        recency,
        frequency,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score::text || '-' || frequency_score::text || '-' || monetary_score::text) AS rfm_code,
        CASE
            WHEN recency_score = 5 AND frequency_score = 5 AND monetary_score = 5 THEN 'Champions'
            WHEN recency_score >= 4 AND frequency_score >= 3 THEN 'Loyal'
            WHEN recency_score = 1 AND frequency_score = 1 AND monetary_score = 1 THEN 'Lost'
            ELSE 'Others'
        END AS segment
    FROM rfm_scores;

-- 6.6 Топ-50 клиентов по выручке
SELECT *
FROM temp_rfm_final
ORDER BY monetary DESC
LIMIT 50;

-- 6.7 Анализ сегментов: количество клиентов, выручка и доля выручки по сегментам
SELECT
    segment,
    COUNT(*) AS customers,
    SUM(monetary) AS revenue,
    ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER() ,2) AS revenue_rate
FROM temp_rfm_final
GROUP BY segment
ORDER BY revenue DESC;

-- =============================================
-- Экспорт агрегатов по сегментам в CSV для построения графиков
-- =============================================
COPY (
    SELECT 
        segment, 
        COUNT(*) AS customers, 
        SUM(monetary) AS revenue
    FROM temp_rfm_final
    GROUP BY segment
) TO 'C:/VS_CODE_PROJECTS/PET_PROJECTS/SQL PROJECT/output_data/rfm_segments.csv' 
WITH CSV HEADER;
-- СОЗДАНИЕ НОРМАЛИЗОВАННОЙ СТРУКТУРЫ БАЗЫ ДАННЫХ
-- Создаем три таблицы в соответствии с 3НФ: продукты, транзакции и позиции заказов
-- Используем SERIAL для автоматической генерации уникальных идентификаторов

-- создание таблицы products
CREATE TABLE products (
    product_sku SERIAL PRIMARY KEY,  -- Автоматически генерируемый уникальный идентификатор товара
    product_id INTEGER,
    brand VARCHAR(50),
    product_line VARCHAR(50),
    product_class VARCHAR(20),
    product_size VARCHAR(20)
);

-- создание таблицы transactions
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,  --  Уникальный идентификатор транзакции
    customer_id INTEGER,			     -- ID клиента (будет связан с таблицей customers)
    transaction_date VARCHAR(20),
    online_order BOOLEAN,
    order_status VARCHAR(20)
);

-- создание таблицы order_line
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,                                -- Уникальный идентификатор позиции заказа
    transaction_id INTEGER REFERENCES transactions(transaction_id),	 -- Ссылка на транзакцию
    product_sku INTEGER REFERENCES products(product_sku),			 -- Ссылка на товар
    list_price NUMERIC(10,2),
    standard_cost NUMERIC(10,2)
);

-- ИСПРАВЛЕНИЕ ТИПА ДАННЫХ ДАТЫ
-- Преобразуем текстовое поле даты в настоящий тип DATE для корректной работы с датами
ALTER TABLE transactions 
ALTER COLUMN transaction_date TYPE DATE 
USING TO_DATE(transaction_date, 'MM/DD/YYYY');

-- СОЗДАНИЕ СТРУКТУРЫ ДЛЯ КЛИЕНТОВ (НОВАЯ ЧАСТЬ СИСТЕМЫ)
-- Основная таблица с информацией о клиентах
CREATE TABLE customers (
    customer_id INTEGER primary key,
    first_name VARCHAR(50) not null,
    last_name VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    deceased_indicator BOOLEAN default false
    );

-- Таблица с профессиональной информацией клиентов
create table customer_job (
    customer_id INTEGER primary key references customers(customer_id), -- Связь с основной таблицей
    job_title VARCHAR(100),
    job_industry_category VARCHAR(50)
    );

-- Таблица с финансовым профилем клиентов
create table customer_financials (
    customer_id integer primary key references customers(customer_id), -- Связь с основной таблицей
    wealth_segment VARCHAR(50),
    owns_car boolean default false,
    property_valution integer check (property_valution between 1 and 10)
    );

-- Таблица с адресной информацией клиентов
create table customer_addresses (
    customer_id integer primary key references customers(customer_id), -- Связь с основной таблицей
    address text,
    postcode varchar(20),
    state varchar(50),
    country varchar(50)
    );

-- ПОДГОТОВКА ВРЕМЕННОЙ ТАБЛИЦЫ ДЛЯ ИМПОРТА ДАННЫХ КЛИЕНТОВ
-- Временная таблица для загрузки сырых данных из CSV перед преобразованием
create table temp_customers_import (
    customer_id INTEGER,
    first_name VARCHAR(50) not null,
    last_name VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    job_title VARCHAR(100),
    job_industry_category VARCHAR(50),
    wealth_segment VARCHAR(50),
    deceased_indicator VARCHAR(1),
    owns_car VARCHAR(3),
    address TEXT,
    postcode VARCHAR(20),
    state VARCHAR(50),
    country VARCHAR(50),
    property_valuation INTEGER
    );

-- ЗАГРУЗКА ДАННЫХ КЛИЕНТОВ В НОРМАЛИЗОВАННУЮ СТРУКТУРУ

-- Основные данные клиентов с преобразованием boolean полей
INSERT INTO customers (customer_id, first_name, last_name, gender, date_of_birth, deceased_indicator)
SELECT 
    customer_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    CASE 
        WHEN deceased_indicator = 'Y' THEN TRUE
        WHEN deceased_indicator = 'N' THEN FALSE
        ELSE FALSE
    END as deceased_indicator
FROM temp_customers_import;

-- 2.2 Профессиональные данные
INSERT INTO customer_job (customer_id, job_title, job_industry_category)
SELECT 
    customer_id,
    job_title,
    job_industry_category
FROM temp_customers_import;

-- Финансовые данные с преобразованием boolean и нормализацией числовых значений
INSERT INTO customer_financials (customer_id, wealth_segment, owns_car, property_valution)
SELECT
    customer_id,
    wealth_segment,
    CASE
        WHEN owns_car = 'Yes' THEN TRUE
        WHEN owns_car = 'No' THEN FALSE
        ELSE FALSE
    END as owns_car,
    CASE
        WHEN property_valuation < 1 THEN 1
        WHEN property_valuation > 10 THEN 10
        ELSE property_valuation
    END as property_valution  
FROM temp_customers_import;

-- Адресные данные 
INSERT INTO customer_addresses (customer_id, address, postcode, state, country)
SELECT 
    customer_id,
    address,
    postcode,
    state,
    country
FROM temp_customers_import;

-- ПРОВЕРКА ЦЕЛОСТНОСТИ СВЯЗЕЙ МЕЖДУ ТРАНЗАКЦИЯМИ И КЛИЕНТАМИ
-- Проверка клиентов, которые есть в транзакциях, но отсутствуют в таблице customers
SELECT COUNT(*) as missing_customers
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL; --получили значение 3, непорядок!

-- Идентификация конкретных отсутствующих клиентов
SELECT DISTINCT t.customer_id
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL; -- нашли потеряшку, клиент с customer_id = 5034

-- Анализ транзакционной активности отсутствующего клиента
SELECT 
    t.customer_id,
    COUNT(*) as transaction_count,
    SUM(oi.list_price) as total_spent
FROM transactions t
JOIN order_items oi ON t.transaction_id = oi.transaction_id
WHERE t.customer_id IN (
    SELECT DISTINCT t.customer_id
    FROM transactions t
    LEFT JOIN customers c ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
)
GROUP BY t.customer_id; -- Клиент 5034 совершил 3 транзакции на сумму 1519.92

-- ВОССТАНОВЛЕНИЕ ЦЕЛОСТНОСТИ ДАННЫХ
-- Добавляем отсутствующего клиента с заглушками для обязательных полей
INSERT INTO customers (customer_id, first_name, last_name, gender, deceased_indicator)
VALUES (5034, 'Unknown', 'Customer', 'Unknown', FALSE);

INSERT INTO customer_job (customer_id, job_title, job_industry_category)
VALUES (5034, 'Unknown', 'Unknown');

INSERT INTO customer_financials (customer_id, wealth_segment, owns_car, property_valution)
VALUES (5034, 'Mass Customer', FALSE, 5);

INSERT INTO customer_addresses (customer_id, address, postcode, state, country)
VALUES (5034, 'Unknown', '0000', 'Unknown', 'Unknown');

-- ПОДТВЕРЖДЕНИЕ ВОССТАНОВЛЕНИЯ ЦЕЛОСТНОСТИ
-- Проверяем, что все клиенты из транзакций теперь есть в таблице customers
SELECT COUNT(*) as missing_customers
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL; -- получили 0 -> потеряный клиент успешно добавлен во все таблицы

-- Демонстрация успешного связывания добавленного клиента с его транзакциями
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(t.transaction_id) as transaction_count
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
WHERE c.customer_id = 5034
GROUP BY c.customer_id, c.first_name, c.last_name; -- Успешно отображаем клиента и его транзакции
    
-- ПОДГОТОВКА ВРЕМЕННОЙ ТАБЛИЦЫ ДЛЯ ИМПОРТА ТРАНЗАКЦИЙ И ТОВАРОВ
-- Временная таблица для загрузки исходных данных о транзакциях и товарах
CREATE TABLE temp_import (
    transaction_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    transaction_date TEXT,
    online_order BOOLEAN,
    order_status VARCHAR(20),
    brand VARCHAR(50),
    product_line VARCHAR(50),
    product_class VARCHAR(20),
    product_size VARCHAR(20),
    list_price NUMERIC(10,2),
    standard_cost NUMERIC(10,2)
);

-- Далее мы импортировали данные из CVS в temp_import
-- с помощью встроенных инструментов DBeaver
-- Настройки импорта: кодировка UTF-8, разделитель запятая, заголовки включены

-- ПРОВЕРКА УСПЕШНОСТИ ИМПОРТА
-- Подсчитываем количество записей чтобы убедиться что все данные загружены
SELECT COUNT(*) as imported_records FROM temp_import; -- результат 20 000, порядок

-- теперь импортируем данные из temp_import в products
INSERT INTO products (product_id, brand, product_line, product_class, product_size)
SELECT DISTINCT -- убираем дубликаты
    product_id,
    brand,
    product_line,
    product_class,
    product_size
FROM temp_import;

-- ПРЕОБРАЗОВАНИЕ И ЗАГРУЗКА ТРАНЗАКЦИЙ С ПАРСИНГОМ ДАТ
-- Обработка дат через CASE и регулярные выражения
-- Регулярка ^\d{1,2}/\d{1,2}/\d{4}$ проверяет формат ММ/ДД/ГГГГ
-- TO_DATE конвертирует валидные строки в тип DATE, невалидные становятся NULL
INSERT INTO transactions (transaction_id, customer_id, transaction_date, online_order, order_status)
SELECT DISTINCT -- убираем дубликаты
    transaction_id,
    customer_id,
    CASE -- преобразование дат через CASE с помощью регулярных выражений
        WHEN transaction_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_DATE(transaction_date, 'MM/DD/YYYY')
        ELSE NULL
    END as transaction_date,
    online_order,
    order_status
FROM temp_import;
			   
-- СОЗДАНИЕ СВЯЗЕЙ МЕЖДУ ТАБЛИЦАМИ ЧЕРЕЗ ПОЗИЦИИ ЗАКАЗОВ
-- JOIN для точного сопоставления товаров по всем характеристикам
-- Сохраняем исторические цены из исходных данных на момент транзакции
INSERT INTO order_items (transaction_id, product_sku, list_price, standard_cost)  -- Вставляем данные в таблицу order_items в указанные столбцы
SELECT                                                                             -- Выбираем данные для вставки
    temp_import.transaction_id,                                                    -- Берем ID транзакции из временной таблицы
    products.product_sku,                                                          -- Берем SKU товара из таблицы продуктов (сгенерированный ключ)
    temp_import.list_price,                                                        -- Берем цену продажи из временной таблицы
    temp_import.standard_cost                                                      -- Берем себестоимость из временной таблицы
FROM temp_import                                                                   -- Из временной таблицы с исходными данными
JOIN products ON temp_import.product_id = products.product_id                      -- Соединяем с таблицей продуктов по ID товара
    AND temp_import.brand = products.brand                                         -- И по бренду (важно, т.к. один product_id может иметь разные бренды)
    AND temp_import.product_line = products.product_line                           -- И по продуктовой линейке
    AND temp_import.product_class = products.product_class                         -- И по классу продукта
    AND temp_import.product_size = products.product_size;                          -- И по размеру продукта (полное совпадение всех характеристик)

-- СОЗДАНИЕ ВНЕШНИХ КЛЮЧЕЙ ДЛЯ ГАРАНТИИ ЦЕЛОСТНОСТИ ДАННЫХ
-- Связываем транзакции с клиентами через внешний ключ
ALTER TABLE transactions 
ADD CONSTRAINT fk_transactions_customers 
FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

-- КОМПЛЕКСНАЯ ПРОВЕРКА ЦЕЛОСТНОСТИ ВСЕХ СВЯЗЕЙ В БАЗЕ ДАННЫХ, все проверки должны вернуть 0
-- Проверка связи транзакций с клиентами
SELECT COUNT(*) as missing_customer_links
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL; -- получили 0

-- Проверка связи позиций заказов с транзакциями
SELECT COUNT(*) as missing_transaction_links
FROM order_items oi
LEFT JOIN transactions t ON oi.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL; -- получили 0

-- Проверка связи позиций заказов с товарами
SELECT COUNT(*) as missing_product_links
FROM order_items oi
LEFT JOIN products p ON oi.product_sku = p.product_sku
WHERE p.product_sku IS NULL; -- получили 0

-- ФИНАЛЬНАЯ КОМПЛЕКСНАЯ СВОДКА ПО СОСТОЯНИЮ БАЗЫ ДАННЫХ
-- Общая статистика по наполненности таблиц и целостности данных
SELECT 
    -- Основные таблицы
    (SELECT COUNT(*) FROM customers) as total_customers,           -- Всего клиентов в системе
    (SELECT COUNT(*) FROM products) as total_products,             -- Всего товаров в каталоге
    (SELECT COUNT(*) FROM transactions) as total_transactions,     -- Всего совершенных транзакций
    (SELECT COUNT(*) FROM order_items) as total_order_items,       -- Всего позиций в заказах
    
    -- Вспомогательные таблицы клиентов
    (SELECT COUNT(*) FROM customer_job) as customer_jobs_records,          -- Записей о профессиях клиентов
    (SELECT COUNT(*) FROM customer_financials) as customer_financials_records,  -- Записей финансовых профилей
    (SELECT COUNT(*) FROM customer_addresses) as customer_addresses_records,    -- Записей адресов клиентов
    
    -- Проверка уникальности и связей данных
    (SELECT COUNT(DISTINCT customer_id) FROM transactions) as customers_with_transactions,  -- Клиенты с хотя бы одной покупкой
    (SELECT COUNT(DISTINCT brand) FROM products) as unique_brands,          -- Уникальных брендов в системе
    (SELECT COUNT(DISTINCT product_line) FROM products) as unique_product_lines  -- Уникальных продуктовых линеек
    

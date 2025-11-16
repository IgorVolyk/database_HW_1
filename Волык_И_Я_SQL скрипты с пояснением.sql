-- СОЗДАНИЕ НОРМАЛИЗОВАННОЙ СТРУКТУРЫ БАЗЫ ДАННЫХ
-- Создаем три таблицы в соответствии с 3НФ: продукты, транзакции и позиции заказов
-- Используем SERIAL для автоматической генерации уникальных идентификаторов
-- создание таблицы products
CREATE TABLE products (
    product_sku SERIAL PRIMARY KEY,
    product_id INTEGER,
    brand VARCHAR(50),
    product_line VARCHAR(50),
    product_class VARCHAR(20),
    product_size VARCHAR(20)
);

-- создание таблицы transactions
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    transaction_date VARCHAR(20),
    online_order BOOLEAN,
    order_status VARCHAR(20)
);

-- создание таблицы order_line
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    transaction_id INTEGER REFERENCES transactions(transaction_id),
    product_sku INTEGER REFERENCES products(product_sku),
    list_price NUMERIC(10,2),
    standard_cost NUMERIC(10,2)
);

-- изменение типа данны в колонке transaction_date таблицы transactions на DATE
ALTER TABLE transactions 
ALTER COLUMN transaction_date TYPE DATE 
USING TO_DATE(transaction_date, 'MM/DD/YYYY');

-- ПОДГОТОВКА ВРЕМЕННОЙ ТАБЛИЦЫ ДЛЯ ИМПОРТА
-- Создаем промежуточную таблицу с текстовыми полями для гибкого импорта CSV
-- Все поля соответствуют исходным данным без преобразований
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
SELECT COUNT(*) as imported_records FROM temp_import;

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
-- JOIN с множественными условиями для точного сопоставления продуктов
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

    
-- ИТОГОВАЯ СВОДКА ПО ПРОЕКТУ
SELECT 
    (SELECT COUNT(*) FROM temp_import) as source_records,           -- Всего записей в исходной таблице
    (SELECT COUNT(*) FROM products) as products_count,              -- Всего записей в таблице продуктов  
    (SELECT COUNT(*) FROM transactions) as transactions_count,      -- Всего записей в таблице транзакций
    (SELECT COUNT(*) FROM order_items) as order_items_count,        -- Всего записей в таблице позиций заказов
    (SELECT COUNT(DISTINCT brand) FROM products) as unique_brands,  -- Уникальных брендов в продуктах 
    (SELECT COUNT(DISTINCT customer_id) FROM transactions) as unique_customers;  -- Уникальных клиентов 
    



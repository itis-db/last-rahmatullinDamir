-- group by

CREATE TABLE sales (
                       region VARCHAR,
                       city VARCHAR,
                       department VARCHAR,
                       amount INT
);

INSERT INTO sales (region, city, department, amount) VALUES
                                                         ('Северо-Запад', 'Санкт-Петербург', 'Электроника', 15000),
                                                         ('Центр', 'Москва', 'Одежда', 9500),
                                                         ('Урал', 'Екатеринбург', 'Продукты', 7600),
                                                         ('Сибирь', 'Новосибирск', 'Косметика', 4300),
                                                         ('Дальний Восток', 'Владивосток', 'Техника', 12000),
                                                         ('Юг', 'Ростов-на-Дону', 'Электроника', 8900),
                                                         ('Центр', 'Москва', 'Продукты', 6700),
                                                         ('Северо-Запад', 'Псков', 'Одежда', 3400),
                                                         ('Урал', 'Челябинск', 'Техника', 11000),
SELECT
    region,
    city,
    GROUPING(region) AS grp_region,
    GROUPING(city) AS grp_city,
    SUM(amount) AS total
FROM sales
GROUP BY ROLLUP(region, city);

-- window

CREATE TABLE sales (
                       employee VARCHAR,
                       month VARCHAR,
                       amount INT
);

INSERT INTO sales VALUES
                      ('Alice', 'Jan', 100),
                      ('Alice', 'Feb', 200),
                      ('Alice', 'Mar', 150),
                      ('Bob', 'Jan', 50),
                      ('Bob', 'Feb', 300),
                      ('Bob', 'Mar', 200);


select employee, sum(amount)
from sales
group by employee;


SELECT employee,
       month,
       amount,
       count(amount) OVER (PARTITION BY employee) as count_per_employee,
       sum(amount) OVER (PARTITION BY employee) as total_per_employee,
       avg(amount) OVER (PARTITION BY employee) as avg_per_employee
FROM sales;

--https://postgrespro.ru/docs/postgresql/17/functions-window
SELECT
    employee,
    month,
    amount,
    ROW_NUMBER() OVER (PARTITION BY employee ORDER BY amount DESC) AS row_num,
    RANK() OVER (PARTITION BY month ORDER BY amount DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY employee ORDER BY amount DESC) AS dense_rank,
    NTILE(4) OVER () AS nt
FROM sales;


SELECT
    employee,
    month,
    amount,
    LAG(amount, 1) OVER w AS prev_month_amount,
    LEAD(amount, 1) OVER w AS next_month_amount,
    FIRST_VALUE(amount) OVER w AS first_amount,
    LAST_VALUE(amount) OVER (PARTITION BY employee) AS last_amount
FROM sales
WINDOW w AS (PARTITION BY employee ORDER BY month DESC);
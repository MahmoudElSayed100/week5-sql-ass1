CREATE SCHEMA reporting_schema;

CREATE TABLE IF NOT EXISTS reporting_schema.agg_daily
(
	day_id SERIAL PRIMARY KEY,
	daily_date DATE,
	daily_rentals INTEGER,
	customers_served INTEGER,
	running_rentals INTEGER,
	prev_day_rentals NUMERIC,
	percent_change NUMERIC,
	avg_rentals_per_customer NUMERIC
);

WITH AGG_RENTALS_CUSTOMERS_CTE AS
(
    SELECT
        CAST(myrental.rental_date AS DATE) AS daily_date,
        COALESCE(COUNT(myrental.rental_id), 0) AS daily_rentals,
        SUM(COALESCE(COUNT(myrental.rental_id), 0)) OVER (ORDER BY CAST(myrental.rental_date AS DATE)) AS running_rentals,
        COALESCE(COUNT(DISTINCT(myrental.customer_id)), 0) AS customers_served
    FROM public.rental AS myrental
    GROUP BY 
        CAST(myrental.rental_date AS DATE)
    ORDER BY 
        CAST(myrental.rental_date AS DATE)
),
PERCENT_CHANGE_CTE AS
(
    SELECT
        daily_date,
        daily_rentals,
        COALESCE(LAG(daily_rentals) OVER (ORDER BY daily_date), 0) AS prev_day_rentals,
        CASE
            WHEN LAG(daily_rentals) OVER (ORDER BY daily_date) IS NOT NULL
            THEN ROUND(
            COALESCE(
                (CAST(daily_rentals AS NUMERIC) - LAG(daily_rentals) OVER (ORDER BY daily_date)) / NULLIF(LAG(daily_rentals) OVER (ORDER BY daily_date), 0) * 100, 0), 1)
            ELSE 0  -- Replace NULL with 0
        END AS percentage_change
    FROM AGG_RENTALS_CUSTOMERS_CTE
),
AVG_RENTALS_CTE AS
(
    SELECT 
        daily_date,
        ROUND(COALESCE(CAST(daily_rentals AS NUMERIC) / NULLIF(customers_served, 0), 0), 1) AS avg_rentals_per_customer
    FROM AGG_RENTALS_CUSTOMERS_CTE
),

DAILY_RESULTS_CTE AS(
	SELECT
		agg_rentals_customers.daily_date, --
		agg_rentals_customers.customers_served,--
		agg_rentals_customers.running_rentals,
		agg_rentals_customers.daily_rentals,--
		percent_change.prev_day_rentals,
		percent_change.percentage_change,--
		avg_rentals.avg_rentals_per_customer---
	FROM AGG_RENTALS_CUSTOMERS_CTE AS agg_rentals_customers
	INNER JOIN 	PERCENT_CHANGE_CTE AS percent_change
	ON agg_rentals_customers.daily_date = percent_change.daily_date
	INNER JOIN AVG_RENTALS_CTE as avg_rentals
	ON percent_change.daily_date = avg_rentals.daily_date
	ORDER BY
		agg_rentals_customers.daily_date
)


INSERT INTO reporting_schema.agg_daily
(
	daily_date,
	daily_rentals,
	customers_served,
	running_rentals,
	prev_day_rentals,
	percent_change,
	avg_rentals_per_customer
)
SELECT
    d.daily_date,
    d.daily_rentals,
    d.customers_served,
	d.running_rentals,
    d.prev_day_rentals,
    d.percentage_change,
    d.avg_rentals_per_customer
FROM DAILY_RESULTS_CTE as d;

SELECT * FROM reporting_schema.agg_daily




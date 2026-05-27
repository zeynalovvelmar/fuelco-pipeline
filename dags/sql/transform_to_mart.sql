INSERT INTO mart.daily_station_summary (
    station_id, 
    sale_date, 
    total_liters, 
    total_revenue, 
    top_fuel, 
    transaction_cnt, 
    updated_at
)
SELECT 
    station_id,
    sale_date,
    SUM(liters) AS total_liters,
    SUM(total_amount) AS total_revenue,
    (
        SELECT fuel_type 
        FROM raw.fuel_sales sub 
        WHERE sub.station_id = main.station_id 
          AND sub.sale_date = main.sale_date 
        GROUP BY fuel_type 
        ORDER BY SUM(liters) DESC 
        LIMIT 1
    ) AS top_fuel,
    COUNT(*) AS transaction_cnt,
    NOW() AS updated_at
FROM raw.fuel_sales main
WHERE sale_date = '{{ ds }}'
GROUP BY station_id, sale_date
ON CONFLICT (station_id, sale_date) 
DO UPDATE SET 
    total_liters = EXCLUDED.total_liters,
    total_revenue = EXCLUDED.total_revenue,
    top_fuel = EXCLUDED.top_fuel,
    transaction_cnt = EXCLUDED.transaction_cnt,
    updated_at = NOW();

-- athena/create_table.sql
-- Create external table for energy data in Athena

CREATE EXTERNAL TABLE IF NOT EXISTS energy_data_db.energy_data (
  DateTime timestamp,
  kWh decimal(15,1)
)
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://s3-for-energy/energy-data/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1',
  'delimiter'=',',
  'classification'='csv'
);

-- athena/create_view.sql
-- Create a view with transformations (alternative to permanent transformations)

CREATE OR REPLACE VIEW energy_data_db.energy_data_view AS
SELECT 
  DateTime,
  ROUND(kWh, 1) as kWh_rounded,
  DATE_FORMAT(DateTime, '%Y-%m-%d') as date_only,
  DATE_FORMAT(DateTime, '%H') as hour_only,
  CASE 
    WHEN DATE_FORMAT(DateTime, '%H') BETWEEN '06' AND '18' THEN 'Day'
    ELSE 'Night'
  END as time_period
FROM energy_data_db.energy_data
WHERE DateTime IS NOT NULL
ORDER BY DateTime DESC;

-- athena/sample_queries.sql
-- Sample analytical queries for energy data

-- Query 1: Daily energy consumption summary
SELECT 
  DATE_FORMAT(DateTime, '%Y-%m-%d') as date,
  COUNT(*) as reading_count,
  AVG(kWh) as avg_kwh,
  MIN(kWh) as min_kwh,
  MAX(kWh) as max_kwh,
  SUM(kWh) as total_kwh
FROM energy_data_db.energy_data
WHERE DateTime >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE_FORMAT(DateTime, '%Y-%m-%d')
ORDER BY date DESC;

-- Query 2: Hourly patterns analysis
SELECT 
  DATE_FORMAT(DateTime, '%H') as hour,
  COUNT(*) as reading_count,
  AVG(kWh) as avg_kwh,
  STDDEV(kWh) as stddev_kwh
FROM energy_data_db.energy_data
WHERE DateTime >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY DATE_FORMAT(DateTime, '%H')
ORDER BY hour;

-- Query 3: Peak consumption identification
SELECT 
  DateTime,
  kWh,
  LAG(kWh) OVER (ORDER BY DateTime) as prev_kwh,
  LEAD(kWh) OVER (ORDER BY DateTime) as next_kwh,
  CASE 
    WHEN kWh > LAG(kWh) OVER (ORDER BY DateTime) 
         AND kWh > LEAD(kWh) OVER (ORDER BY DateTime) 
    THEN 'Peak'
    ELSE 'Normal'
  END as consumption_type
FROM energy_data_db.energy_data
WHERE DateTime >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY DateTime DESC;

-- Query 4: Data quality check
SELECT 
  DATE_FORMAT(DateTime, '%Y-%m-%d') as date,
  COUNT(*) as total_records,
  COUNT(CASE WHEN kWh IS NULL THEN 1 END) as null_kwh_count,
  COUNT(CASE WHEN kWh < 0 THEN 1 END) as negative_kwh_count,
  COUNT(CASE WHEN DateTime IS NULL THEN 1 END) as null_datetime_count,
  MIN(DateTime) as earliest_time,
  MAX(DateTime) as latest_time
FROM energy_data_db.energy_data
WHERE DateTime >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY DATE_FORMAT(DateTime, '%Y-%m-%d')
ORDER BY date DESC;

-- Query 5: Monthly energy trends
SELECT 
  DATE_FORMAT(DateTime, '%Y-%m') as month,
  COUNT(*) as reading_count,
  AVG(kWh) as avg_kwh,
  SUM(kWh) as total_kwh,
  MIN(kWh) as min_kwh,
  MAX(kWh) as max_kwh,
  PERCENTILE_APPROX(kWh, 0.5) as median_kwh,
  PERCENTILE_APPROX(kWh, 0.95) as p95_kwh
FROM energy_data_db.energy_data
WHERE DateTime >= CURRENT_DATE - INTERVAL '12' MONTH
GROUP BY DATE_FORMAT(DateTime, '%Y-%m')
ORDER BY month DESC;
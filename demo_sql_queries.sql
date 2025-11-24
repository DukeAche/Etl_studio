-- No-Code ETL Studio - SQL Demo Queries
-- Use these queries to test the SQL Workbench functionality

-- =============================================
-- BASIC DATA EXPLORATION
-- =============================================

-- 1. Preview first 10 records
SELECT * FROM df LIMIT 10;

-- 2. Check data quality - missing values
SELECT 
    'Total Records' as metric, 
    COUNT(*) as value 
FROM df
UNION ALL
SELECT 
    'Missing Age Values', 
    SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END)
FROM df;

-- 3. Data distribution by city
SELECT 
    city,
    COUNT(*) as customer_count,
    ROUND(AVG(total_spent), 2) as avg_spending,
    ROUND(AVG(age), 1) as avg_age
FROM df 
GROUP BY city 
ORDER BY customer_count DESC;

-- =============================================
-- DATA CLEANING QUERIES
-- =============================================

-- 4. Remove duplicates and clean whitespace
SELECT DISTINCT
    customer_id,
    TRIM(customer_name) as clean_customer_name,
    email,
    registration_date,
    age,
    city,
    total_purchases,
    total_spent,
    last_purchase_date,
    is_active
FROM df
WHERE customer_name IS NOT NULL;

-- 5. Handle missing values - fill with median age
SELECT 
    customer_id,
    customer_name,
    email,
    registration_date,
    COALESCE(age, (SELECT ROUND(AVG(age), 0) FROM df WHERE age IS NOT NULL)) as age_filled,
    city,
    total_purchases,
    total_spent,
    last_purchase_date,
    is_active
FROM df;

-- =============================================
-- BUSINESS ANALYTICS QUERIES
-- =============================================

-- 6. High-value customers analysis
SELECT 
    customer_name,
    city,
    age,
    total_purchases,
    total_spent,
    ROUND(total_spent / total_purchases, 2) as avg_order_value,
    CASE 
        WHEN total_spent >= 2500 THEN 'High Value'
        WHEN total_spent >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM df 
WHERE age IS NOT NULL
ORDER BY total_spent DESC
LIMIT 20;

-- 7. Customer activity analysis by age groups
SELECT 
    CASE 
        WHEN age < 30 THEN '18-29'
        WHEN age < 40 THEN '30-39'
        WHEN age < 50 THEN '40-49'
        WHEN age < 60 THEN '50-59'
        ELSE '60+'
    END as age_group,
    COUNT(*) as customer_count,
    SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_customers,
    ROUND(AVG(total_spent), 2) as avg_spending,
    ROUND(100.0 * SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) / COUNT(*), 1) as active_percentage
FROM df 
WHERE age IS NOT NULL
GROUP BY age_group
ORDER BY age_group;

-- =============================================
-- ADVANCED TRANSFORMATIONS
-- =============================================

-- 8. Customer lifetime value calculation
WITH customer_metrics AS (
    SELECT 
        customer_id,
        customer_name,
        city,
        age,
        total_purchases,
        total_spent,
        registration_date,
        last_purchase_date,
        is_active,
        -- Calculate days since registration
        (CURRENT_DATE - registration_date) as days_since_registration,
        -- Calculate days since last purchase
        (CURRENT_DATE - last_purchase_date) as days_since_last_purchase
    FROM df
)
SELECT 
    customer_id,
    customer_name,
    city,
    age,
    total_purchases,
    total_spent,
    ROUND(total_spent / NULLIF(total_purchases, 0), 2) as avg_order_value,
    days_since_registration,
    days_since_last_purchase,
    CASE 
        WHEN days_since_last_purchase <= 30 THEN 'Active'
        WHEN days_since_last_purchase <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as activity_status,
    is_active
FROM customer_metrics
ORDER BY total_spent DESC;

-- 9. Monthly registration trends
SELECT 
    DATE_TRUNC('month', registration_date) as registration_month,
    COUNT(*) as new_customers,
    ROUND(AVG(total_spent), 2) as avg_lifetime_value,
    SUM(total_spent) as total_revenue
FROM df 
GROUP BY DATE_TRUNC('month', registration_date)
ORDER BY registration_month;

-- 10. Export ready - final cleaned dataset
SELECT 
    ROW_NUMBER() OVER (ORDER BY customer_id) as clean_customer_id,
    TRIM(customer_name) as customer_name,
    LOWER(email) as email,
    registration_date,
    COALESCE(age, (SELECT ROUND(AVG(age), 0) FROM df WHERE age IS NOT NULL)) as age,
    city,
    total_purchases,
    ROUND(total_spent, 2) as total_spent,
    last_purchase_date,
    is_active,
    CURRENT_DATE as processed_date,
    'ETL_Studio' as data_source
FROM df 
WHERE customer_name IS NOT NULL
ORDER BY customer_id;
CREATE OR REPLACE PROCEDURE schema_analytics_l1.sp_archive_historical_shortages_v3()
	LANGUAGE plpgsql
AS $$
	
BEGIN

    RAISE INFO 'Starting historical records processing (7D Containment Window)...';

    DROP TABLE IF EXISTS tmp_new_shortages;
    DROP TABLE IF EXISTS tmp_open_history;
    DROP TABLE IF EXISTS tmp_to_extend;
    DROP TABLE IF EXISTS tmp_to_insert;

    CREATE TEMP TABLE tmp_new_shortages AS
    WITH distance_calculation AS (
        SELECT 
            supplier_id, facility_id, product_code, snapshot_timestamp,
            LAG(snapshot_timestamp) OVER (
                PARTITION BY supplier_id, facility_id, product_code 
                ORDER BY snapshot_timestamp ASC
            ) AS previous_timestamp
        FROM schema_analytics_l1.effective_shortages_7d
        WHERE facility_id IS NOT NULL AND TRIM(facility_id) <> ''
    ),
    gap_detection AS (
        SELECT 
            *,
            CASE 
                WHEN previous_timestamp IS NULL THEN 1
                WHEN snapshot_timestamp > previous_timestamp + INTERVAL '13 hour' THEN 1
                ELSE 0 
            END AS is_new_period_start
        FROM distance_calculation
    ),
    group_assignment AS (
        SELECT 
            *,
            SUM(is_new_period_start) OVER (
                PARTITION BY supplier_id, facility_id, product_code 
                ORDER BY snapshot_timestamp ASC
                ROWS UNBOUNDED PRECEDING
            ) AS period_group_id
        FROM gap_detection
    )
    SELECT DISTINCT
        supplier_id, facility_id, product_code,
        MIN(snapshot_timestamp) AS start_date,
        MAX(snapshot_timestamp) AS end_date
    FROM group_assignment
    GROUP BY supplier_id, facility_id, product_code, period_group_id;

    CREATE TEMP TABLE tmp_open_history AS
    WITH latest_records AS (
        SELECT 
            supplier_id, facility_id, product_code, start_date, end_date,
            ROW_NUMBER() OVER (
                PARTITION BY supplier_id, facility_id, product_code 
                ORDER BY end_date DESC, start_date DESC
            ) AS rn
        FROM schema_analytics_l1.historical_shortages
        WHERE end_date >= CURRENT_DATE - INTERVAL '7 day'
          AND end_date >= start_date
    )
    SELECT supplier_id, facility_id, product_code, start_date, end_date
    FROM latest_records
    WHERE rn = 1;

    CREATE TEMP TABLE tmp_to_extend AS
    WITH sorted_new_records AS (
        SELECT supplier_id, facility_id, product_code, start_date, end_date,
               ROW_NUMBER() OVER (
                   PARTITION BY supplier_id, facility_id, product_code 
                   ORDER BY start_date ASC
               ) AS rn
        FROM tmp_new_shortages
    )
    SELECT DISTINCT
        s.supplier_id, s.facility_id, s.product_code,
        s.start_date AS history_start_date,
        n.end_date AS new_end_date
    FROM sorted_new_records n
    INNER JOIN tmp_open_history s
        ON n.supplier_id = s.supplier_id
       AND n.facility_id = s.facility_id
       AND n.product_code = s.product_code
    WHERE n.rn = 1 
      AND n.start_date <= s.end_date + INTERVAL '13 hour'
      AND n.end_date >= s.start_date;

    CREATE TEMP TABLE tmp_to_insert AS
    SELECT DISTINCT
        n.supplier_id, n.facility_id, n.product_code, n.start_date, n.end_date
    FROM tmp_new_shortages n
    WHERE n.end_date >= n.start_date 
      AND NOT EXISTS (
        SELECT 1 
        FROM tmp_to_extend e
        WHERE n.supplier_id = e.supplier_id
          AND n.facility_id = e.facility_id
          AND n.product_code = e.product_code
          AND n.end_date = e.new_end_date
      )
      AND NOT EXISTS (
        SELECT 1 
        FROM schema_analytics_l1.historical_shortages s
        WHERE n.supplier_id = s.supplier_id
          AND n.facility_id = s.facility_id
          AND n.product_code = s.product_code
          AND s.start_date <= n.start_date
          AND s.end_date >= n.end_date 
      );

    RAISE INFO 'Executing updates for history expansion...';
    UPDATE schema_analytics_l1.historical_shortages
    SET end_date = t.new_end_date
    FROM tmp_to_extend t
    WHERE historical_shortages.supplier_id = t.supplier_id
      AND historical_shortages.facility_id = t.facility_id
      AND historical_shortages.product_code = t.product_code
      AND historical_shortages.start_date = t.history_start_date;

    RAISE INFO 'Executing inserts for new standalone periods...';
    INSERT INTO schema_analytics_l1.historical_shortages (
        supplier_id, facility_id, product_code, start_date, end_date
    )
    SELECT supplier_id, facility_id, product_code, start_date, end_date
    FROM tmp_to_insert;

    DROP TABLE IF EXISTS tmp_new_shortages;
    DROP TABLE IF EXISTS tmp_open_history;
    DROP TABLE IF EXISTS tmp_to_extend;
    DROP TABLE IF EXISTS tmp_to_insert;

    RAISE INFO 'Job execution completed successfully.';

END;

$$
;

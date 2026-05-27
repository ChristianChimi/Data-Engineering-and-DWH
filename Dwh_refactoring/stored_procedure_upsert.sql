CREATE OR REPLACE PROCEDURE layer1_5_core.sp_upsert_sales_orders_lines()
	LANGUAGE plpgsql
AS $$
	
DECLARE
    v_active_deleted    BIGINT;
    v_active_inserted   BIGINT;
    v_history_inserted  BIGINT;
BEGIN
    RAISE INFO 'sp_upsert_sales_orders_lines: execution started';

    DROP TABLE IF EXISTS tmp_active_sales_lines_dedup;

    CREATE TEMP TABLE tmp_active_sales_lines_dedup
    DISTKEY (customer_code)
    SORTKEY (branch_code)
    AS
    SELECT
        src_branch as branch_code,
        src_hist_id as history_prog_id,
        src_order_id  as order_prog_id,
        src_cust_id  as customer_code,
        src_delivery_id as delivery_note_id,
        src_line_id as order_line_id,
        is_fulfilled as is_fulfilled,
        src_prod_id as product_code,
        src_prod_desc as product_description,
        uom  as unit_of_measure,
        qty_requested as requested_quantity,
        qty_ordered as ordered_quantity,
        qty_ordered_cev as ordered_quantity_cev,
        qty_delivered  as delivered_quantity,
        qty_delivered_cev as delivered_quantity_cev,
        selling_price  as selling_price,
        retail_price  as public_retail_price,
        vat_rate as vat_rate,
        discount_pct_line as line_discount_pct,
        discount_pct_cond1 as cond_discount_pct_1,
        discount_pct_cond2  as cond_discount_pct_2,
        discount_pct_cust as client_discount_pct,
        discount_pct_cust1 as client_discount_pct_1,
        discount_pct_cust2 as client_discount_pct_2,
        discount_pct_prod  as product_discount_pct,
        product_type  as product_type,
        warehouse_id  as warehouse_code,
        product_status as product_status_code,
        qty_reversed  as reversed_quantity,
        qty_reversed_cev as reversed_quantity_cev,
        container_ref as container_reference,
        record_timestamp as record_timestamp
    FROM (
        SELECT
            src_branch, src_cust_id, src_hist_id, src_order_id, src_delivery_id, src_line_id, is_fulfilled, src_prod_id,
            src_prod_desc, uom, qty_requested, qty_ordered, qty_ordered_cev, qty_delivered, qty_delivered_cev, selling_price,
            retail_price, vat_rate, discount_pct_line, discount_pct_cond1, discount_pct_cond2, discount_pct_cust, discount_pct_cust1, discount_pct_cust2,
            discount_pct_prod, product_type, warehouse_id, product_status, qty_reversed, qty_reversed_cev, container_ref,
            record_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY src_branch, src_cust_id, src_order_id, src_delivery_id, src_line_id, src_prod_id
                ORDER BY record_timestamp DESC
            ) AS rn
        FROM layer1_staging.stg_sl_012_r
    )
    WHERE rn = 1;

    DELETE FROM layer1_5_core.sales_orders_lines
    WHERE history_prog_id = 0;

    GET DIAGNOSTICS v_active_deleted = ROW_COUNT;
    RAISE INFO 'Active Partition: rows deleted = %', v_active_deleted;

    INSERT INTO layer1_5_core.sales_orders_lines (
        branch_code, history_prog_id, order_prog_id, customer_code, delivery_note_id, order_line_id,
        is_fulfilled, product_code, product_description, unit_of_measure, requested_quantity, ordered_quantity, 
        ordered_quantity_cev, delivered_quantity, delivered_quantity_cev, selling_price, public_retail_price, 
        vat_rate, line_discount_pct, cond_discount_pct_1, cond_discount_pct_2, client_discount_pct, 
        client_discount_pct_1, client_discount_pct_2, product_discount_pct, product_type, warehouse_code, 
        product_status_code, reversed_quantity, reversed_quantity_cev, container_reference, record_timestamp
    )
    SELECT
        branch_code, history_prog_id, order_prog_id, customer_code, delivery_note_id, order_line_id,
        is_fulfilled, product_code, product_description, unit_of_measure, requested_quantity, ordered_quantity, 
        ordered_quantity_cev, delivered_quantity, delivered_quantity_cev, selling_price, public_retail_price, 
        vat_rate, line_discount_pct, cond_discount_pct_1, cond_discount_pct_2, client_discount_pct, 
        client_discount_pct_1, client_discount_pct_2, product_discount_pct, product_type, warehouse_code, 
        product_status_code, reversed_quantity, reversed_quantity_cev, container_reference, record_timestamp
    FROM tmp_active_sales_lines_dedup;

    GET DIAGNOSTICS v_active_inserted = ROW_COUNT;
    RAISE INFO 'Active Partition: rows inserted = %', v_active_inserted;


    DROP TABLE IF EXISTS tmp_history_sales_lines_dedup;

    CREATE TEMP TABLE tmp_history_sales_lines_dedup
    DISTKEY (customer_code)
    SORTKEY (branch_code)
    AS
    SELECT
        src_branch as branch_code,
        src_hist_id as history_prog_id,
        src_order_id  as order_prog_id,
        src_cust_id  as customer_code,
        src_delivery_id as delivery_note_id,
        src_line_id as order_line_id,
        is_fulfilled as is_fulfilled,
        src_prod_id as product_code,
        src_prod_desc as product_description,
        uom  as unit_of_measure,
        qty_requested as requested_quantity,
        qty_ordered as ordered_quantity,
        qty_ordered_cev as ordered_quantity_cev,
        qty_delivered  as delivered_quantity,
        qty_delivered_cev as delivered_quantity_cev,
        selling_price  as selling_price,
        retail_price  as public_retail_price,
        vat_rate as vat_rate,
        discount_pct_line as line_discount_pct,
        discount_pct_cond1 as cond_discount_pct_1,
        discount_pct_cond2  as cond_discount_pct_2,
        discount_pct_cust as client_discount_pct,
        discount_pct_cust1 as client_discount_pct_1,
        discount_pct_cust2 as client_discount_pct_2,
        discount_pct_prod  as product_discount_pct,
        product_type  as product_type,
        warehouse_id  as warehouse_code,
        product_status as product_status_code,
        qty_reversed  as reversed_quantity,
        qty_reversed_cev as reversed_quantity_cev,
        container_ref as container_reference,
        record_timestamp as record_timestamp
    FROM (
        SELECT
            src_branch, src_cust_id, src_hist_id, src_order_id, src_delivery_id, src_line_id, is_fulfilled, src_prod_id,
            src_prod_desc, uom, qty_requested, qty_ordered, qty_ordered_cev, qty_delivered, qty_delivered_cev, selling_price,
            retail_price, vat_rate, discount_pct_line, discount_pct_cond1, discount_pct_cond2, discount_pct_cust, discount_pct_cust1, discount_pct_cust2,
            discount_pct_prod, product_type, warehouse_id, product_status, qty_reversed, qty_reversed_cev, container_ref,
            record_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY src_branch, src_cust_id, src_hist_id, src_order_id, src_delivery_id, src_line_id, src_prod_id
                ORDER BY record_timestamp DESC
            ) AS rn
        FROM layer1_staging.stg_sl_022_r
    )
    WHERE rn = 1;


    INSERT INTO layer1_5_core.sales_orders_lines (
        branch_code, history_prog_id, order_prog_id, customer_code, delivery_note_id, order_line_id,
        is_fulfilled, product_code, product_description, unit_of_measure, requested_quantity, ordered_quantity, 
        ordered_quantity_cev, delivered_quantity, delivered_quantity_cev, selling_price, public_retail_price, 
        vat_rate, line_discount_pct, cond_discount_pct_1, cond_discount_pct_2, client_discount_pct, 
        client_discount_pct_1, client_discount_pct_2, product_discount_pct, product_type, warehouse_code, 
        product_status_code, reversed_quantity, reversed_quantity_cev, container_reference, record_timestamp
    )
    SELECT
        s.branch_code, s.history_prog_id, s.order_prog_id, s.customer_code, s.delivery_note_id, s.order_line_id,
        s.is_fulfilled, s.product_code, s.product_description, s.unit_of_measure, s.requested_quantity, s.ordered_quantity, 
        s.ordered_quantity_cev, s.delivered_quantity, s.delivered_quantity_cev, s.selling_price, s.public_retail_price, 
        s.vat_rate, s.line_discount_pct, s.cond_discount_pct_1, s.cond_discount_pct_2, s.client_discount_pct, 
        s.client_discount_pct_1, s.client_discount_pct_2, s.product_discount_pct, s.product_type, s.warehouse_code, 
        s.product_status_code, s.reversed_quantity, s.reversed_quantity_cev, s.container_reference, s.record_timestamp
    FROM tmp_history_sales_lines_dedup s
    LEFT JOIN layer1_5_core.sales_orders_lines t
        ON  t.branch_code       = s.branch_code
        AND t.customer_code     = s.customer_code
        AND t.history_prog_id   = s.history_prog_id
        AND t.order_prog_id     = s.order_prog_id
        AND t.delivery_note_id  = s.delivery_note_id
        AND t.order_line_id     = s.order_line_id
        AND t.product_code      = s.product_code
    WHERE t.branch_code IS NULL;

    GET DIAGNOSTICS v_history_inserted = ROW_COUNT;
    RAISE INFO 'History Partition: rows inserted = %', v_history_inserted;


    DROP TABLE IF EXISTS tmp_active_sales_lines_dedup;
    DROP TABLE IF EXISTS tmp_history_sales_lines_dedup;

    RAISE INFO 'sp_upsert_sales_orders_lines: execution finalized successfully';

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'sp_upsert_sales_orders_lines FAILED: %', SQLERRM;
END;

$$
;
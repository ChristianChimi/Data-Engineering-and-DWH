CREATE OR REPLACE MATERIALIEZ VIEW layer2_marts.vw_sales_orders_extended
AS ( 
    -- SALES ORDERS EXTENDED ANALYTICAL VIEW
    SELECT
        -- Surrogate Keys / Join Keys Lineage Generation
        a.branch_code || '_' || a.customer_code || '_' || a.history_prog_id || '_' || a.order_prog_id || '_' || a.order_line_id || '_' || a.product_code AS fulfillment_surrogate_key,
        a.branch_code || '_' || a.customer_code || '_' || a.history_prog_id || '_' || a.order_prog_id || '_' || a.delivery_note_id AS customer_order_surrogate_key,
        a.branch_code || '_' || a.customer_code || '_' || a.history_prog_id || '_' || a.order_prog_id || '_' || a.container_reference || '_' || CASE WHEN a.container_reference = LOWER(a.container_reference) THEN 'low' ELSE 'upp' END AS container_order_surrogate_key,
        a.customer_code || '_' || a.shipping_address AS shipping_destination_surrogate_key,
        
        -- Granular Attributes & Dimensions
        a.branch_code,
        a.history_prog_id,
        a.order_prog_id,
        a.customer_code,
        a.delivery_note_id,
        a.order_line_id,
        a.order_type,
        a.product_code,
        a.product_description,
        a.unit_of_measure,
        a.product_type,
        a.is_bulky_item,
        a.container_count,
        a.is_refrigerated,
        a.shipping_line,
        a.shipping_address,
        a.container_reference,
        
        -- Date Elements
        a.order_date,
        a.delivery_note_date,
        a.delivery_date,
        a.fulfillment_closed_timestamp,
        
        -- Execution Flags
        a.is_fulfillment_generated,
        a.is_fulfilled,
        a.is_order_consolidated,
        a.delivery_note_number,
        
        -- Quantitative & Financial Metrics
        a.requested_quantity,
        a.ordered_quantity,
        a.ordered_quantity_cev,
        a.delivered_quantity,
        a.delivered_quantity_cev,
        a.reversed_quantity,
        a.reversed_quantity_cev,
        a.selling_price,
        a.public_retail_price,
        a.vat_rate,
        a.line_discount_pct,
        a.cond_discount_pct_1,
        a.cond_discount_pct_2,
        a.client_discount_pct,
        a.client_discount_pct_1,
        a.client_discount_pct_2,
        a.product_discount_pct,
        a.lines_to_fulfill,
        a.lines_ordered,
        a.record_timestamp,
        
        -- Consolidated Push/Pull Omni-Channel Metrics from Subquery (b)
        b.delivered_qty_push,
        b.ordered_qty_push,
        b.delivered_qty_retail,
        b.ordered_qty_retail,
        b.delivered_qty_b2b,
        b.ordered_qty_b2b,
        b.delivered_qty_other,
        b.ordered_qty_other,
        
        -- Complex Order Channel Determination Logic
        CASE
            WHEN b.branch_code IS NULL THEN a.order_channel
            WHEN (
                (CASE WHEN COALESCE(b.ordered_qty_b2b,    0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_push,   0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_retail, 0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_other,  0) > 0 THEN 1 ELSE 0 END)
            ) = 0 THEN a.order_channel
            WHEN (
                (CASE WHEN COALESCE(b.ordered_qty_b2b,    0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_push,   0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_retail, 0) > 0 THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(b.ordered_qty_other,  0) > 0 THEN 1 ELSE 0 END)
            ) = 1 THEN
                CASE
                    WHEN COALESCE(b.ordered_qty_b2b,    0) > 0 THEN 'B2B'
                    WHEN COALESCE(b.ordered_qty_push,   0) > 0 THEN 'Push'
                    WHEN COALESCE(b.ordered_qty_retail, 0) > 0 THEN 'Retail'
                    WHEN COALESCE(b.ordered_qty_other,  0) > 0 THEN 'Other'
                END
            ELSE
                'Mixed (' ||
                RTRIM(
                    (CASE WHEN COALESCE(b.ordered_qty_b2b,    0) > 0 THEN 'B2B, '    ELSE '' END) ||
                    (CASE WHEN COALESCE(b.ordered_qty_push,   0) > 0 THEN 'Push, '   ELSE '' END) ||
                    (CASE WHEN COALESCE(b.ordered_qty_retail, 0) > 0 THEN 'Retail, ' ELSE '' END) ||
                    (CASE WHEN COALESCE(b.ordered_qty_other,  0) > 0 THEN 'Other, '  ELSE '' END),
                    ', '
                ) || ')'
        END AS order_channel,
        
        -- Time-Intelligence Derivations (BI Optimized)
        DATE_PART(year, a.delivery_note_date) AS delivery_note_year,
        DATE_PART(month, a.delivery_note_date) AS delivery_note_month,
        CASE
            WHEN DATE_PART(month, a.delivery_note_date) = 1  THEN 'January'
            WHEN DATE_PART(month, a.delivery_note_date) = 2  THEN 'February'
            WHEN DATE_PART(month, a.delivery_note_date) = 3  THEN 'March'
            WHEN DATE_PART(month, a.delivery_note_date) = 4  THEN 'April'
            WHEN DATE_PART(month, a.delivery_note_date) = 5  THEN 'May'
            WHEN DATE_PART(month, a.delivery_note_date) = 6  THEN 'June'
            WHEN DATE_PART(month, a.delivery_note_date) = 7  THEN 'July'
            WHEN DATE_PART(month, a.delivery_note_date) = 8  THEN 'August'
            WHEN DATE_PART(month, a.delivery_note_date) = 9  THEN 'September'
            WHEN DATE_PART(month, a.delivery_note_date) = 10 THEN 'October'
            WHEN DATE_PART(month, a.delivery_note_date) = 11 THEN 'November'
            WHEN DATE_PART(month, a.delivery_note_date) = 12 THEN 'December'
            ELSE NULL 
        END AS delivery_note_month_name,
        
        CASE WHEN a.delivered_quantity > 0 THEN 1 ELSE 0 END AS is_delivered_flag
    FROM layer2_marts.mvw_sales_orders a
    LEFT JOIN (
        SELECT 
            branch_code,
            history_prog_id,
            order_prog_id,
            customer_code,
            delivery_note_id,
            SUM(delivered_qty_push)   AS delivered_qty_push,
            SUM(ordered_qty_push)     AS ordered_qty_push,
            SUM(delivered_qty_retail) AS delivered_qty_retail,
            SUM(ordered_qty_retail)   AS ordered_qty_retail,
            SUM(delivered_qty_b2b)    AS delivered_qty_b2b,
            SUM(ordered_qty_b2b)      AS ordered_qty_b2b,
            SUM(delivered_qty_other)  AS delivered_qty_other,
            SUM(ordered_qty_other)    AS ordered_qty_other
        FROM layer2_marts.mvw_consolidated_sales_orders
        GROUP BY 1,2,3,4,5
    ) b
        ON  a.branch_code     = b.branch_code
        AND a.history_prog_id = b.history_prog_id
        AND a.order_prog_id   = b.order_prog_id
        AND a.customer_code   = b.customer_code
        AND a.delivery_note_id = b.delivery_note_id
    WHERE a.order_date >= (CURRENT_DATE - 1 - INTERVAL '12 months')
)
WITH NO SCHEMA BINDING;
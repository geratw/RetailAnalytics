CREATE OR REPLACE FUNCTION cross_selling(num_groups INT, max_churn NUMERIC,
                                             max_stab NUMERIC, max_sku NUMERIC, marg_share NUMERIC)
    RETURNS TABLE
            (
                Customer_ID          INT,
                SKU_Name             VARCHAR,
                Offer_Discount_Depth NUMERIC
            )
AS
$$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS group_select AS
    SELECT gr.customer_id,
           gr.group_id
    FROM (
             SELECT *,
                    row_number()
                    OVER (PARTITION BY g.customer_id, g.group_id ORDER BY g.group_affinity_index DESC) AS COUNT
             FROM groups g
         ) AS gr
    WHERE COUNT <= num_groups
      AND group_churn_rate <= max_churn
      AND group_stability_index < max_stab;

    CREATE TEMP TABLE IF NOT EXISTS sku_information AS
    SELECT c.customer_id,
           sku_id,
           customer_primary_store,
           sku_retail_price - sku_purchase_price AS difference,
           group_id
    FROM customers c
             JOIN stores s ON c.customer_primary_store = s.transaction_store_id
             JOIN group_select gs ON c.customer_id = gs.customer_id
    ORDER BY customer_id, difference DESC;

    CREATE TEMP TABLE IF NOT EXISTS max_margin_sku AS
    SELECT sp.customer_id,
           sp.group_id,
           sp.customer_primary_store,
           MAX(sku_retail_price - sku_purchase_price) AS max_marg
    FROM sku_information sp
             JOIN stores ON sp.sku_id = stores.sku_id
             JOIN product_grid pg ON stores.sku_id = pg.sku_id
    GROUP BY sp.customer_id, sp.group_id, sp.customer_primary_store;

    CREATE TEMP TABLE IF NOT EXISTS count_transactions_sku AS
    SELECT DISTINCT sm.customer_id, COUNT(c.transaction_id) AS count_sku
    FROM max_margin_sku sm
             JOIN purchase_history ph ON sm.customer_id = ph.customer_id AND sm.group_id = ph.group_id
             JOIN checks c ON ph.transaction_id = c.transaction_id
    GROUP BY sm.customer_id, c.sku_id;

    CREATE TEMP TABLE IF NOT EXISTS count_transactions_group AS
    SELECT sm.customer_id,
           COUNT(transaction_id) AS count_group
    FROM max_margin_sku sm
             JOIN purchase_history ph ON sm.customer_id = ph.customer_id AND sm.group_id = ph.group_id
    GROUP BY sm.customer_id, ph.group_id;

    CREATE TEMP TABLE IF NOT EXISTS margin_share AS
    SELECT cs.customer_id,
           count_sku / count_group::NUMERIC AS sku_share
    FROM count_transactions_sku cs
             JOIN count_transactions_group cg ON cs.customer_id = cg.customer_id
    WHERE count_sku / count_group::NUMERIC <= max_sku / 100;

    RETURN QUERY
        SELECT sm.customer_id,
               pg.sku_name,
               (CASE
                    WHEN ((marg_share / 100.0) * max_marg) / sku_retail_price >=
                         ceil(group_minimum_discount * 100 / 5) * 5 / 100
                        THEN ceil(group_minimum_discount * 100 / 5::NUMERIC) * 5 END) AS Offer_Discount_Depth
        FROM max_margin_sku sm
                 JOIN margin_share ms ON sm.customer_id = ms.customer_id
                 JOIN product_grid pg ON sm.group_id = pg.group_id
                 JOIN groups g ON sm.group_id = g.group_id AND sm.customer_id = g.customer_id
                 JOIN stores s ON pg.sku_id = s.sku_id AND sm.customer_primary_store = s.transaction_store_id;

    DROP TABLE IF EXISTS group_select;
    DROP TABLE IF EXISTS sku_peer;
    DROP TABLE IF EXISTS sku_max;
    DROP TABLE IF EXISTS count_trans_sku;
    DROP TABLE IF EXISTS count_trans_group;
    DROP TABLE IF EXISTS margin_share;
END
$$ LANGUAGE plpgsql;

SELECT *
FROM cross_selling(5, 3, 0.5, 100, 30);



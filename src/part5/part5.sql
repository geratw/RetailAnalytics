CREATE
OR REPLACE FUNCTION get_terms_of_the_offer(start_date timestamp, end_date timestamp, add_trans INT)
    RETURNS TABLE
            (
                Customer_ID_terms           INT,
                Required_Transactions_Count NUMERIC
            )
AS
$$
BEGIN
RETURN QUERY
SELECT customer_id,
       ROUND(EXTRACT(EPOCH FROM (end_date -
                                 start_date)) /
             customer_frequency) + add_trans
FROM customers;
END;
$$
LANGUAGE plpgsql;

CREATE
OR REPLACE FUNCTION get_personal_offer(start_date_ DATE, end_date_ DATE, add_trans_ INT, churn_rate_ NUMERIC,
                                              share_of_transaction_ NUMERIC,
                                              share_of_margin_ NUMERIC) -- churn -индекс оттока
    RETURNS TABLE
            (
                "Customer_ID"                 INT,
                "Start_Date"                  DATE,
                "End_Date"                    DATE,
                "Required_Transactions_Count" NUMERIC,
                "Group_Name"                  VARCHAR,
                "Offer_Discount_Depth"        NUMERIC

            )
AS
$$
BEGIN
RETURN QUERY WITH final_table AS (
            WITH help_table AS (
                SELECT customer_id,
                       g.group_id,
                       sg.group_name,
                       ROW_NUMBER() OVER (PARTITION BY customer_id) AS top,
                       g.group_minimum_discount,
                       g.group_margin
                FROM groups g
                         JOIN sku_group sg ON g.group_id = sg.group_id
                WHERE group_churn_rate <= churn_rate_
                  AND group_discount_share < share_of_transaction_ / 100.0
                ORDER BY g.customer_id, group_affinity_index DESC),
                 max_margin_table AS (
                     SELECT customer_id,
                            group_id,
                            share_of_margin_ * 1.0 / 100 * SUM(group_summ - group_cost) / SUM(group_summ) AS discount
                     FROM purchase_history
                     GROUP BY customer_id, group_id)
            SELECT help_table.customer_id,
                   help_table.group_id,
                   group_name,
                   top,
                   ROUND(discount / 0.05) * 5               AS discount,
                   ROUND(group_minimum_discount / 0.05) * 5 AS group_min_discount,
                   MIN(top) OVER (PARTITION BY help_table.customer_id) AS min_top
            FROM help_table
                     JOIN max_margin_table ON help_table.customer_id = max_margin_table.customer_id AND
                                              help_table.group_id = max_margin_table.group_id
            WHERE discount > group_minimum_discount)
SELECT customer_id, start_date_, end_date_, Required_Transactions_Count, group_name, group_min_discount
FROM final_table
         JOIN get_terms_of_the_offer(start_date_, end_date_, add_trans_) g
              ON g.Customer_ID_terms = customer_id
WHERE group_min_discount > 0
  AND top = min_top;
END;
$$
LANGUAGE plpgsql;


SELECT *
FROM get_personal_offer('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1, 3, 70, 30);

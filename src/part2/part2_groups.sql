-------------------------------------SUPPORT_FOR_3RD_COL

CREATE
OR REPLACE FUNCTION count_transactions_in_certain_period(cust_id INT, first_date TIMESTAMPTZ, last_date TIMESTAMPTZ)
    RETURNS INT AS
$$
SELECT COUNT(DISTINCT transaction_id)
FROM purchase_history
WHERE customer_id = cust_id
  AND TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') >= first_date
  AND TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') <= last_date;
$$
LANGUAGE sql;

-----------------------------------------5 col (Group_Stability_Index)

CREATE
OR REPLACE FUNCTION calculate_stability_index()
    RETURNS TABLE
            (
                "CustomerId"      INT,
                "GroupId"         INT,
                calculated_stability_index NUMERIC
            )
AS
$$
BEGIN
RETURN QUERY WITH help_table AS (
            SELECT customer_id,
                   group_id,
                   transaction_datetime,
                   LAG(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))
                   OVER (PARTITION BY customer_id, group_id ORDER BY TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') ASC) AS prev_transaction_datetime
            FROM purchase_history
            ORDER BY customer_id, group_id, TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') ASC)
SELECT help_table.customer_id,
       help_table.group_id,
       AVG(ABS(EXTRACT(EPOCH FROM
                       (TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') -
                        prev_transaction_datetime))::DECIMAL /
                       60 / 60 / 24 - group_frequency::DECIMAL) / group_frequency ::DECIMAL) AS Group_Stability_Index
FROM help_table
         JOIN periods ON help_table.customer_id = periods.customer_id AND help_table.group_id = periods.group_id
GROUP BY help_table.customer_id, help_table.group_id;
END;
$$
LANGUAGE plpgsql;

-----------------------------------------6 col (Group_Margin)

CREATE
OR REPLACE FUNCTION calculate_margin(
    analysis_method VARCHAR,
    analysis_param INT
)
    RETURNS TABLE
            (
                "CustomerId"       INT,
                "GroupId"          INT,
                calculated_margin NUMERIC
            )
AS
$$
BEGIN
    IF
analysis_method = 'transactions' THEN
        RETURN QUERY
            WITH first_method AS (
                SELECT customer_id,
                       group_id,
                       transaction_datetime,
                       ROW_NUMBER()
                       OVER (PARTITION BY customer_id, group_id ORDER BY TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC) AS top,
                       Group_Summ_Paid,
                       Group_Cost
                FROM purchase_history
                ORDER BY customer_id, group_id, TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC)
SELECT first_method.customer_id, first_method.group_id, SUM(Group_Summ_Paid - Group_Cost)
FROM first_method
WHERE top <= analysis_param
GROUP BY customer_id, group_id;

ELSIF
analysis_method = 'period' THEN
        RETURN QUERY
            WITH second_method AS (
                SELECT customer_id,
                       group_id,
                       transaction_datetime,
                       ROW_NUMBER()
                       OVER (PARTITION BY customer_id, group_id ORDER BY TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC) AS top,
                       Group_Summ_Paid,
                       Group_Cost
                FROM purchase_history
                ORDER BY customer_id, group_id, TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC)
SELECT customer_id, group_id, SUM(Group_Summ_Paid - Group_Cost)
FROM second_method
WHERE TO_TIMESTAMP(get_date_of_analysis_formation(), 'DD.MM.YYYY HH24:MI:SS') - INTERVAL '1 day' * analysis_param <=
    TO_TIMESTAMP(transaction_datetime
    , 'DD.MM.YYYY HH24:MI:SS')
  AND TO_TIMESTAMP(get_date_of_analysis_formation()
    , 'DD.MM.YYYY HH24:MI:SS') >=
    TO_TIMESTAMP(transaction_datetime
    , 'DD.MM.YYYY HH24:MI:SS')
GROUP BY customer_id, group_id;
END IF;
END;
$$
LANGUAGE plpgsql;

-----------------------------------------7 col / 9 cols

CREATE
OR REPLACE FUNCTION calculate_group_average_discount_and_share()
    RETURNS TABLE
            (
                "CustomerId"               INT,
                "GroupId"                  INT,
                 Calculated_Group_Discount_Share      NUMERIC,
                 Calculated_Group_Average_Discount    NUMERIC
            )
AS
$$
BEGIN
RETURN QUERY WITH count_amount_with_discont AS (
            SELECT customer_id,
                   purchase_history.group_id,
                   COUNT(c.transaction_id)                    AS amount,
                   SUM(Group_Summ_Paid) / SUM(Group_Summ) AS Group_Average_Discount
            FROM purchase_history
                     JOIN product_grid pg ON purchase_history.group_id = pg.group_id
                     JOIN checks c ON purchase_history.transaction_id = c.transaction_id AND pg.sku_id = c.sku_id
            WHERE sku_discount > 0
            GROUP BY customer_id, purchase_history.group_id
            ORDER BY customer_id, group_id)
SELECT periods.customer_id,
       periods.group_id,
       amount / group_purchase::NUMERIC AS Group_Discount_Share, Group_Average_Discount
FROM periods
         LEFT JOIN count_amount_with_discont c
                   ON c.group_id = periods.group_id AND c.customer_id = periods.customer_id;
END;
$$
LANGUAGE plpgsql;

-----------------------------------------8 col / переработано

CREATE
OR REPLACE FUNCTION calculate_group_min_discount()
    RETURNS TABLE
            (
                "CustomerId"               INT,
                "GroupId"                  INT,
                 Calculated_Group_Minimum_Discount      NUMERIC
            )
AS
$$
BEGIN
RETURN QUERY
SELECT customer_id,
       group_id,
       CASE WHEN MIN(group_min_discount) = 0 THEN NULL ELSE MIN(group_min_discount) END AS min_discount
FROM periods
GROUP BY customer_id, group_id;
END;
$$
LANGUAGE plpgsql;

-----------------------------------------------------MAIN

CREATE
OR REPLACE VIEW Groups AS
SELECT customer_id,
       group_id,
       group_purchase::NUMERIC / count_transactions_in_certain_period(customer_id, First_Group_Purchase_Date,
                                                                      Last_Group_Purchase_Date) AS Group_Affinity_Index,
            EXTRACT(EPOCH FROM
                    (TO_TIMESTAMP(get_date_of_analysis_formation(), 'DD.MM.YYYY HH24:MI:SS') -
                     Last_Group_Purchase_Date)) /
            60 / 60 / 24 / periods.group_frequency AS Group_Churn_Rate,
       calculated_stability_index        AS Group_Stability_Index,
       calculated_margin                 AS Group_Margin,
       Calculated_Group_Discount_Share   AS Group_Discount_Share,
       Calculated_Group_Minimum_Discount AS Group_Minimum_Discount,
       Calculated_Group_Average_Discount AS Group_Average_Discount
FROM periods
         JOIN calculate_stability_index() s_i ON customer_id = s_i."CustomerId" AND group_id = s_i."GroupId"
         JOIN calculate_margin('transactions', 60000) c_m ON customer_id = c_m."CustomerId" AND group_id = c_m."GroupId"
         JOIN calculate_group_average_discount_and_share() c_ds
              ON customer_id = c_ds."CustomerId" AND group_id = c_ds."GroupId"
         JOIN calculate_group_min_discount() c_min ON customer_id = c_min."CustomerId" AND group_id = c_min."GroupId";


-----------------------------------------
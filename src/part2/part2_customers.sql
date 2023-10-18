-- SUPPORT FOR DATE

CREATE
OR REPLACE FUNCTION get_date_of_analysis_formation()
    RETURNS VARCHAR AS
$$
SELECT MAX(analysis_formation)
FROM date_of_analysis_formation;
$$
LANGUAGE sql;

-- SUPPORT FOR MAIN STORE

CREATE
OR REPLACE FUNCTION get_main_store()
    RETURNS TABLE
            (
                customer_id INT,
                prime_store INT
            )
AS
$$
WITH transtaction_procent AS (SELECT c.customer_id,
                                     transaction_store_id,
                                     COUNT(*)::NUMERIC /
                                     (SELECT COUNT(c2.customer_id)::NUMERIC
                                      FROM personal_information
                                               JOIN cards c2 ON personal_information.customer_id = c2.customer_id
                                               JOIN transactions t2 ON c2.customer_card_id = t2.customer_card_id
                                      WHERE c2.customer_id = c.customer_id)                           AS orders_percent,
                                     MAX(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) AS last_time_order
                              FROM personal_information
                                       JOIN cards c ON personal_information.customer_id = c.customer_id
                                       JOIN transactions t ON c.customer_card_id = t.customer_card_id
                              GROUP BY c.customer_id, transaction_store_id
                              ORDER BY c.customer_id),
     first_prime_store AS (
         WITH primary_store_by_last_3 AS (SELECT c.customer_id,
                                                 transaction_store_id,
                                                 ROW_NUMBER()
                                                 OVER (PARTITION BY personal_information.customer_id ORDER BY TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC) AS top
                                          FROM personal_information
                                                   JOIN cards c ON personal_information.customer_id = c.customer_id
                                                   JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                          ORDER BY c.customer_id)
         SELECT customer_id,
                CASE
                    WHEN
                        COUNT(DISTINCT transaction_store_id) = 1 THEN MAX(transaction_store_id)
                    END AS primary_store
         FROM primary_store_by_last_3
         WHERE top <= 3
         GROUP BY customer_id
     ),
     help_table AS (
         SELECT f.customer_id,
                transaction_store_id,
                orders_percent,
                last_time_order,
                primary_store,
                ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY orders_percent DESC, last_time_order DESC) AS top
         FROM transtaction_procent t
                  LEFT JOIN first_prime_store f ON t.customer_id = f.customer_id)
SELECT help_table.customer_id, COALESCE(primary_store, transaction_store_id) AS prime_store
FROM help_table
WHERE top = 1;
$$
LANGUAGE sql;

-- MAIN

CREATE
OR REPLACE VIEW Customers AS
WITH cust_id_and_avg_check AS (
    SELECT personal_information.customer_id                                 AS Customer_ID,
           AVG(t.transaction_summ)                                          AS Customer_Average_Check,
           EXTRACT(EPOCH FROM (MAX(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) -
                               MIN(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')))) /
           COUNT(transaction_datetime) / 60 / 60 / 24                       AS Customer_Frequency,
           MAX(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) AS last_transaction_date
    FROM personal_information
             JOIN cards c ON personal_information.customer_id = c.customer_id
             JOIN transactions t ON c.customer_card_id = t.customer_card_id
    GROUP BY personal_information.customer_id),
     first_six_cols AS (
         SELECT Customer_ID,
                Customer_Average_Check,
                CASE
                    WHEN PERCENT_RANK() OVER (ORDER BY Customer_Average_Check DESC) <= 0.1 THEN 'High'
                    WHEN PERCENT_RANK() OVER (ORDER BY Customer_Average_Check DESC) <= 0.35 THEN 'Medium'
                    ELSE 'Low'
                    END                                                    AS Customer_Average_Check_Segment,
                Customer_Frequency,
                CASE
                    WHEN PERCENT_RANK() OVER (ORDER BY Customer_Frequency ASC) <= 0.1 THEN 'Often'
                    WHEN PERCENT_RANK() OVER (ORDER BY Customer_Frequency ASC) <= 0.35 THEN 'Occasionally'
                    ELSE 'Rarely'
                    END                                                    AS Customer_Frequency_Segment,
                EXTRACT(EPOCH FROM (to_timestamp(get_date_of_analysis_formation(), 'DD.MM.YYYY HH24:MI:SS') -
                                    last_transaction_date)) / 60 / 60 / 24 AS Customer_Inactive_Period
         FROM cust_id_and_avg_check),
     first_eight_cols AS (
         SELECT *,
                Customer_Inactive_Period / Customer_Frequency AS Customer_Churn_Rate,
                CASE
                    WHEN Customer_Inactive_Period / Customer_Frequency >= 0 AND
                         Customer_Inactive_Period / Customer_Frequency < 2 THEN 'Low'
                    WHEN Customer_Inactive_Period / Customer_Frequency >= 2 AND
                         Customer_Inactive_Period / Customer_Frequency < 5 THEN 'Medium'
                    ELSE 'High'
                    END                                       AS Customer_Churn_Segment
         FROM first_six_cols)
SELECT first_eight_cols.Customer_ID,
       Customer_Average_Check,
       Customer_Average_Check_Segment,
       Customer_Frequency,
       Customer_Frequency_Segment,
       Customer_Inactive_Period,
       Customer_Churn_Rate,
       Customer_Churn_Segment,
       CASE Customer_Average_Check_Segment
           WHEN 'Low' THEN 0
           WHEN 'Medium' THEN 9
           ELSE 18 END +
       CASE Customer_Frequency_Segment
           WHEN 'Rarely' THEN 0
           WHEN 'Occasionally' THEN 3
           ELSE 6 END +
       CASE Customer_Churn_Segment
           WHEN 'Low' THEN 1
           WHEN 'Medium' THEN 2
           ELSE 3
           END     AS Customer_Segment,
       prime_store AS Customer_Primary_Store
FROM first_eight_cols
         JOIN get_main_store() ON get_main_store.customer_id = first_eight_cols.Customer_ID
ORDER BY first_eight_cols.Customer_ID;
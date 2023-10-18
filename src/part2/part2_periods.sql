CREATE
OR REPLACE VIEW Periods AS
SELECT ph.Customer_ID,
       ph.Group_ID,
       MIN(TO_TIMESTAMP(ph.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))                      AS First_Group_Purchase_Date,
       MAX(TO_TIMESTAMP(ph.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))                      AS Last_Group_Purchase_Date,
       COUNT(DISTINCT ph.transaction_id)                                                        AS Group_Purchase,
       (EXTRACT(EPOCH FROM (MAX(TO_TIMESTAMP(ph.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) -
                            MIN(TO_TIMESTAMP(ph.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')))) / 60 / 60 / 24 + 1) /
       COUNT(DISTINCT ph.transaction_id)                                                        AS Group_Frequency,
       COALESCE(MIN(CASE WHEN SKU_Discount / SKU_Summ > 0 THEN SKU_Discount / SKU_Summ END), 0) AS Group_Min_Discount
FROM purchase_history AS ph
         JOIN product_grid pg ON ph.group_id = pg.group_id
         JOIN Checks AS CK ON CK.Transaction_ID = ph.Transaction_ID AND pg.sku_id = CK.sku_id
GROUP BY ph.Customer_ID, ph.Group_ID
ORDER BY customer_id;
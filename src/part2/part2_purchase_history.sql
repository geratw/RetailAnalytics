CREATE
OR REPLACE VIEW Purchase_History AS
WITH PH AS (
    SELECT CR.Customer_ID,
           TR.Transaction_ID,
           TR.Transaction_DateTime,
           SKU.Group_ID,
           CK.SKU_Amount,
           SR.SKU_Purchase_Price,
           CK.SKU_Summ_Paid,
           CK.SKU_Summ
    FROM Transactions AS TR
             JOIN Cards AS CR ON CR.Customer_Card_ID = TR.Customer_Card_ID
             JOIN personal_information AS PD ON PD.Customer_ID = CR.Customer_ID
             JOIN Checks AS CK ON TR.Transaction_ID = CK.Transaction_ID
             JOIN product_grid AS SKU ON SKU.SKU_ID = CK.SKU_ID
             JOIN Stores AS SR ON SKU.SKU_ID = SR.SKU_ID
        AND TR.Transaction_Store_ID = SR.Transaction_Store_ID
)
SELECT PH.Customer_ID,
       PH.Transaction_ID,
       PH.Transaction_DateTime,
       PH.Group_ID,
       SUM(PH.SKU_Purchase_Price * PH.SKU_Amount) AS Group_Cost,
       SUM(PH.SKU_Summ)                           AS Group_Summ,
       SUM(PH.SKU_Summ_Paid)                      AS Group_Summ_Paid
FROM PH
GROUP BY PH.Customer_ID, PH.Transaction_ID, PH.Transaction_DateTime, PH.Group_ID;

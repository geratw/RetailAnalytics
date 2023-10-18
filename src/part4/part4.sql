--1.1
CREATE OR REPLACE FUNCTION fnc_method_period(first_date DATE DEFAULT NULL, last_date DATE DEFAULT NULL)
    RETURNS TABLE
            (
                customer    INT,
                transaction INT,
                summa       NUMERIC
            )
AS
$$
DECLARE
    first_date_analysis DATE = TO_DATE((SELECT transaction_datetime
                                        FROM transactions
                                        ORDER BY TO_DATE(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')
                                        LIMIT 1), 'DD.MM.YYYY HH24:MI:SS');
    last_date_analysis  DATE = TO_DATE((SELECT transaction_datetime
                                        FROM transactions
                                        ORDER BY TO_DATE(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC
                                        LIMIT 1), 'DD.MM.YYYY HH24:MI:SS');
BEGIN
    IF (first_date IS NULL OR last_date IS NULL OR first_date >= last_date) THEN
        RAISE EXCEPTION 'incorrect dates';
    END IF;
    IF (first_date > first_date_analysis AND first_date < last_date_analysis) THEN
        first_date_analysis = first_date;
    END IF;
    IF (last_date > first_date_analysis AND last_date < last_date_analysis) THEN
        last_date_analysis = last_date;
    END IF;
    RETURN QUERY
        SELECT p.customer_id, t.transaction_id, t.transaction_summ
        FROM personal_information p
                 INNER JOIN cards c ON p.customer_id = c.customer_id
                 INNER JOIN transactions t ON c.customer_card_id = t.customer_card_id
        WHERE TO_DATE(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') BETWEEN first_date_analysis AND last_date_analysis;
END;
$$ LANGUAGE plpgsql;

--1.2
CREATE OR REPLACE FUNCTION fnc_method_last_transaction(count_transaction INT DEFAULT NULL)
    RETURNS TABLE
            (
                customer    INT,
                transaction INT,
                summa       NUMERIC
            )
AS
$$
BEGIN
    IF (count_transaction IS NULL OR count_transaction <= 0) THEN
        RAISE EXCEPTION 'incorrect count';
    END IF;
    RETURN QUERY
        WITH last_transactions AS (
            SELECT p.customer_id,
                   t.transaction_id,
                   t.transaction_summ,
                   ROW_NUMBER()
                   OVER (PARTITION BY c.customer_id ORDER BY TO_DATE(t.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC) AS count_id
            FROM personal_information p
                     INNER JOIN cards c ON p.customer_id = c.customer_id
                     INNER JOIN transactions t ON c.customer_card_id = t.customer_card_id
        )
        SELECT customer_id,
               transaction_id,
               transaction_summ
        FROM last_transactions
        WHERE count_id <= count_transaction;
END;
$$ LANGUAGE plpgsql;

--2
CREATE OR REPLACE FUNCTION fnc_avg_check(
    method_ INT,
    first_date_ DATE DEFAULT NULL,
    last_date_ DATE DEFAULT NULL,
    count_transaction_ INT DEFAULT NULL
)
    RETURNS TABLE
            (
                customer_id INT,
                avg_check   NUMERIC
            )
AS
$$
BEGIN
    IF method_ = 1 THEN
        RETURN QUERY
            WITH method_period_tab AS (
                SELECT *
                FROM fnc_method_period(first_date_, last_date_)
            ),
                 customer_avg AS (
                     SELECT customer, SUM(summa) / COUNT(*) AS avg_check
                     FROM method_period_tab
                     GROUP BY customer
                 )
            SELECT m.customer,
                   c.avg_check
            FROM method_period_tab m
                     JOIN customer_avg c ON m.customer = c.customer;
    ELSIF method_ = 2 THEN
        RETURN QUERY
            WITH method_period_tab AS (
                SELECT *
                FROM fnc_method_last_transaction(count_transaction_)
            ),
                 customer_avg AS (
                     SELECT customer, SUM(summa) / COUNT(*) AS avg_check
                     FROM method_period_tab
                     GROUP BY customer
                 )
            SELECT m.customer,
                   c.avg_check
            FROM method_period_tab m
                     JOIN customer_avg c ON m.customer = c.customer;
    ELSE
        RAISE EXCEPTION 'incorrect method';
    END IF;
END;
$$ LANGUAGE plpgsql;

--3
CREATE OR REPLACE FUNCTION fnc_target_avg_check(
    method_ INT DEFAULT 2,
    first_date_ DATE DEFAULT NULL,
    last_date_ DATE DEFAULT NULL,
    count_transaction_ INT DEFAULT 100,
    k_increase_avg_check_ NUMERIC DEFAULT 1.15
)
    RETURNS TABLE
            (
                customer_id      INT,
                target_avg_check NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT d.customer_id, d.avg_check * k_increase_avg_check_ AS target_avg_check
        FROM fnc_avg_check(method_, first_date_, last_date_, count_transaction_) d
        ORDER BY customer_id;
END;
$$ LANGUAGE plpgsql;

--4
CREATE OR REPLACE FUNCTION fnc_find_group(
    churn_rate_ NUMERIC DEFAULT 3,
    discount_share_ NUMERIC DEFAULT 70
)
    RETURNS TABLE
            (
                customer_ INT,
                group_    INT,
                name      VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT customer_id,
               g.group_id,
               sg.group_name
        FROM groups g
                 JOIN sku_group sg ON g.group_id = sg.group_id
        WHERE group_churn_rate <= churn_rate_
          AND group_discount_share < discount_share_ / 100
        ORDER BY g.customer_id, group_affinity_index DESC;
END;
$$ LANGUAGE plpgsql;

--5
CREATE OR REPLACE FUNCTION fnc_max_sale(
    churn_rate_ NUMERIC DEFAULT 3,
    discount_share_ NUMERIC DEFAULT 70,
    margin_ NUMERIC DEFAULT 30
)
    RETURNS TABLE
            (
                customer   INT,
                "group"    INT,
                name       VARCHAR,
                max_margin NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH max_margin_table AS (
            SELECT customer_id,
                   group_id,
                   margin_ * 1.0 / 100 * SUM(group_summ - group_cost) / SUM(group_summ) AS discount
            FROM purchase_history
            GROUP BY customer_id, group_id
        )
        SELECT customer_, group_, exp.name, discount
        FROM fnc_find_group(churn_rate_, discount_share_) exp
                 INNER JOIN max_margin_table h ON exp.customer_ = h.customer_id AND exp.group_ = h.group_id;
END;
$$ LANGUAGE plpgsql;

--6
CREATE OR REPLACE FUNCTION fnc_sale(
    churn_rate_ NUMERIC DEFAULT 3,
    discount_share_ NUMERIC DEFAULT 70,
    margin_ NUMERIC DEFAULT 30
)
    RETURNS TABLE
            (
                customer_  INT,
                name       VARCHAR,
                max_margin NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT s.customer, s.name, CEIL(s.group_min_discount / 0.05) * 5
        FROM (
                 SELECT exp.customer,
                        exp.name,
                        exp.max_margin,
                        p.group_min_discount,
                        ROW_NUMBER()
                        OVER (PARTITION BY customer) AS count_id
                 FROM fnc_max_sale(churn_rate_, discount_share_, margin_) exp
                          INNER JOIN periods p ON exp.customer = p.customer_id AND exp.group = p.group_id
                 WHERE CEIL(p.group_min_discount / 0.05) * 0.05 < exp.max_margin
             ) s
        WHERE s.count_id = 1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_part4(
    method_ INT DEFAULT 2,
    first_date_ DATE DEFAULT NULL,
    last_date_ DATE DEFAULT NULL,
    count_transaction_ INT DEFAULT 100,
    k_increase_avg_check_ NUMERIC DEFAULT 1.15,
    churn_rate_ NUMERIC DEFAULT 3,
    discount_share_ NUMERIC DEFAULT 70,
    margin_ NUMERIC DEFAULT 30
)
    RETURNS TABLE
            (
                customer_id            INT,
                required_check_measure NUMERIC,
                group_name             VARCHAR,
                offer_discount_depth   NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH table_conditions AS (
            SELECT *
            FROM fnc_target_avg_check(method_, first_date_, last_date_, count_transaction_, k_increase_avg_check_)
        ),
             table_reward AS (
                 SELECT *
                 FROM fnc_sale(churn_rate_, discount_share_, margin_)
             )
        SELECT c.customer_id, ROUND(c.target_avg_check, 2), r.name, r.max_margin
        FROM table_conditions c
                 JOIN table_reward r ON c.customer_id = r.customer_;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_part4();
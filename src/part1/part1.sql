CREATE TABLE IF NOT EXISTS personal_information
(
    customer_id            SERIAL PRIMARY KEY,
    customer_name          VARCHAR,
    customer_surname       VARCHAR,
    customer_primary_email VARCHAR,
    customer_primary_phone VARCHAR,
    CONSTRAINT ch_customer_primary_email CHECK (customer_primary_email ~ '^[^ ]+@[a-zA-Z0-9]+\.[a-zA-Z]{2,}$'),
    CONSTRAINT ch_customer_primary_phone CHECK (customer_primary_phone ~ '^\+7\d{10}$'),
    CONSTRAINT ch_customer_customer_name CHECK (customer_name ~ '(^[A-Z][a-z\-\ ]*$|^[А-Я][а-я\-\ ]*$)'),
    CONSTRAINT ch_customer_customer_surname CHECK (customer_surname ~ '(^[A-Z][a-z\-\ ]*$|^[А-Я][а-я\-\ ]*$)')
);

CREATE TABLE IF NOT EXISTS cards
(
    customer_card_id SERIAL PRIMARY KEY,
    customer_id      INT,
    CONSTRAINT fk_cards_customer_id FOREIGN KEY (customer_id) REFERENCES personal_information (customer_id)
);

CREATE TABLE IF NOT EXISTS sku_group
(
    group_id   SERIAL PRIMARY KEY,
    group_name VARCHAR,
    CONSTRAINT ch_sku_group_group_name CHECK (group_name ~ '^[^ ]*$')
);

CREATE TABLE IF NOT EXISTS product_grid
(
    sku_id   SERIAL PRIMARY KEY,
    sku_name VARCHAR,
    group_id INT,
    CONSTRAINT fk_product_grid_group_id FOREIGN KEY (group_id) REFERENCES sku_group (group_id)
);

CREATE TABLE IF NOT EXISTS stores
(
    transaction_store_id INT,
    sku_id               INT,
    sku_purchase_price   NUMERIC,
    sku_retail_price     NUMERIC,
    CONSTRAINT fk_stores_sku_id FOREIGN KEY (sku_id) REFERENCES product_grid (sku_id)
);

CREATE TABLE IF NOT EXISTS transactions
(
    transaction_id       SERIAL PRIMARY KEY,
    customer_card_id     INT,
    transaction_summ     NUMERIC,
    transaction_datetime VARCHAR,
    transaction_store_id INT,
    CONSTRAINT fk_transactions_customer_card_id FOREIGN KEY (customer_card_id) REFERENCES cards (customer_card_id),
    CONSTRAINT ch_transactions_transaction_datetime CHECK (
                transaction_datetime ~ '^\d{2}\.\d{2}\.\d{4} \d{1,2}:\d{2}:\d{2}$'
            AND TO_DATE(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') IS NOT NULL
        )
);

CREATE TABLE IF NOT EXISTS checks
(
    transaction_id INT,
    sku_id         INT,
    sku_amount     NUMERIC,
    sku_summ       NUMERIC,
    sku_summ_paid  NUMERIC,
    sku_discount   NUMERIC,
    CONSTRAINT fk_checks_transaction_id FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id),
    CONSTRAINT fk_checks_sku_id FOREIGN KEY (sku_id) REFERENCES product_grid (sku_id)
);

CREATE TABLE IF NOT EXISTS date_of_analysis_formation
(
    analysis_formation VARCHAR,
    CONSTRAINT ch_analysis_formation CHECK (
                analysis_formation ~ '^\d{2}\.\d{2}\.\d{4} \d{1,2}:\d{2}:\d{2}$'
            AND TO_DATE(analysis_formation, 'DD.MM.YYYY HH24:MI:SS') IS NOT NULL
        )
);

CREATE OR REPLACE PROCEDURE export_tsv()
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE 'COPY cards TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/cards.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY checks TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/checks.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY date_of_analysis_formation TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/date_of_analysis_formation.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY personal_information TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/personal_information.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY product_grid TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/product_grid.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY sku_group TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/sku_group.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY stores TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/stores.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY transactions TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/transactions.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
END;
$$;

CREATE OR REPLACE PROCEDURE import_tsv()
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE 'COPY personal_information FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/personal_information.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY cards FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/cards.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY sku_group FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/sku_group.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY product_grid FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/product_grid.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY stores FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/stores.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY transactions FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/transactions.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY checks FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/checks.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
    EXECUTE 'COPY date_of_analysis_formation FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/tsv/date_of_analysis_formation.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER);';
END;
$$;

CREATE OR REPLACE PROCEDURE export_csv(separator TEXT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE 'COPY cards TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/cards.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY checks TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/checks.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY date_of_analysis_formation TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/date_of_analysis_formation.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY personal_information TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/personal_information.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY product_grid TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/product_grid.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
    EXECUTE 'COPY sku_group TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/sku_group.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY stores TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/stores.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
    EXECUTE 'COPY transactions TO ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/transactions.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
END;
$$;

CREATE OR REPLACE PROCEDURE import_csv(separator TEXT)
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE 'COPY personal_information FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/personal_information.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY cards FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/cards.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY sku_group FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/sku_group.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY product_grid FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/product_grid.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
    EXECUTE 'COPY stores FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/stores.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
    EXECUTE 'COPY transactions FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/transactions.csv'' DELIMITER ''' || separator || ''' CSV HEADER;';
    EXECUTE 'COPY checks FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/checks.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
    EXECUTE 'COPY date_of_analysis_formation FROM ''/Users/' || current_user ||
            '/SQL3_RetailAnalitycs_v1.0-1/src/csv/date_of_analysis_formation.csv'' DELIMITER ''' ||
            separator || ''' CSV HEADER;';
END;
$$;

-- CALL export_csv(',');
-- CALL import_csv(',');
-- CALL export_tsv();
-- CALL import_tsv();

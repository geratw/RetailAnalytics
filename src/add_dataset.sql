CREATE OR REPLACE PROCEDURE import_tsv_dataset(
    option VARCHAR = 'big'
)
    LANGUAGE plpgsql
AS
$$
BEGIN
    TRUNCATE TABLE personal_information CASCADE;
    TRUNCATE TABLE cards CASCADE;
    TRUNCATE TABLE checks CASCADE;
    TRUNCATE TABLE date_of_analysis_formation CASCADE;
    TRUNCATE TABLE product_grid CASCADE;
    TRUNCATE TABLE sku_group CASCADE;
    TRUNCATE TABLE stores CASCADE;
    TRUNCATE TABLE transactions CASCADE;
    IF option = 'big' THEN
        EXECUTE 'COPY personal_information FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Personal_Data.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY cards FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Cards.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY sku_group FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Groups_SKU.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY product_grid FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/SKU.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY stores FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Stores.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY transactions FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Transactions.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY checks FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Checks.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY date_of_analysis_formation FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Date_Of_Analysis_Formation.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
    ELSE
        EXECUTE 'COPY personal_information FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Personal_Data_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY cards FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Cards_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY sku_group FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Groups_SKU_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY product_grid FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/SKU_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY stores FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Stores_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY transactions FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Transactions_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY checks FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Checks_Mini.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
        EXECUTE 'COPY date_of_analysis_formation FROM ''/Users/' || current_user ||
                '/SQL3_RetailAnalitycs_v1.0-1/datasets/Date_Of_Analysis_Formation.tsv'' WITH (FORMAT CSV, DELIMITER E''\t'', HEADER FALSE);';
    END IF;
END;
$$;
CALL import_tsv_dataset('1');
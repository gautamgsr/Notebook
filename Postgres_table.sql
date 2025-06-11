---bottom_1pct_weights
--it contains customer present in bottom 1 percent avg weight.
--since weights of the customer is same throughout the process/
DROP TABLE IF EXISTS bottom_1pct_weights;
CREATE TABLE bottom_1pct_weights AS (
  SELECT *
  FROM (
    SELECT
      ci.customer,
      ci.merchant,
      AVG(ci.weight) AS avg_weight,
      PERCENT_RANK() OVER (PARTITION BY ci.merchant ORDER BY AVG(ci.weight)) AS weight_percentile
    FROM customer_importance ci
    GROUP BY ci.customer, ci.merchant
  ) ranked
  WHERE weight_percentile <= 0.01
);


--chunk_tracking
--This table is used to sync both consumer and producer process
--They make sure chunks are processed in sequnce and write status if written,processed,failed for each chunk
 CREATE TABLE chunk_tracking (
     id SERIAL PRIMARY KEY,
     chunk_index INTEGER,
     s3_path TEXT,
     record_count INTEGER,
     status TEXT,
     created_at TIMESTAMP,
     updated_at TIMESTAMP
);
select * from chunk_tracking;


--customer_merchant_txn_count
--This table keep track of number of transections occuring between merchant and customer
--it updates the avg txn value, txn_count every time new chunk is processed.
CREATE TABLE customer_merchant_txn_counts(
	customer varchar NULL,
	merchant varchar NULL,
	txn_count int8 NULL,
	avg_amount float8 NULL
);
select * from customer_merchant_txn_counts; 

INSERT INTO customer_merchant_txn_counts (customer, merchant, txn_count, avg_amount)
SELECT customer, merchant, txn_count, avg_amount
FROM staging_customer_merchant_txn_counts
ON CONFLICT (customer, merchant)
DO UPDATE 
SET txn_count = customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count,
    avg_amount = (
    (customer_merchant_txn_counts.avg_amount * customer_merchant_txn_counts.txn_count + EXCLUDED.avg_amount * EXCLUDED.txn_count) /
    (customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count)
    );


--merchant_gender_count
--This table keep track of counts of males and females to each merchant
--it updates the number of male and females to each merchant every time new chunk is processed. 
create table merchant_gender_counts(
	merchant varchar,
	male     integer,
	female   integer
);
ALTER TABLE merchant_gender_counts
ADD CONSTRAINT unique_merchant UNIQUE (merchant);

INSERT INTO merchant_gender_counts (merchant,male,female)
SELECT merchant, male, female
FROM staging_merchant_gender_counts
ON CONFLICT (merchant)
DO UPDATE 
SET male   = merchant_gender_counts.male + EXCLUDED.male,
    female = merchant_gender_counts.female + EXCLUDED.female;


select * from merchant_gender_counts;


--final_output
--it store the final data which is merchant,customer,pattern find
--it gets deduped every time new chunk is processed and keeps the oldest row.
CREATE TABLE final_output(
    ystarttime TIMESTAMP,
    detectiontime TIMESTAMP,
    patternid varchar,
    actiontype varchar,
    customer varchar,
    merchant varchar,
    published varchar
);
CREATE INDEX idx_final_output_cust_merchant_time
ON final_output (customer, merchant, ystarttime);

----to dedupe final_output
WITH ranked_rows AS (
    					SELECT ctid,
        						ROW_NUMBER() OVER (
            					PARTITION BY customer, merchant
            					ORDER BY ystarttime ASC) AS rn
    					 FROM final_output)
                        DELETE FROM final_output
                        WHERE ctid IN ( SELECT ctid FROM ranked_rows WHERE rn > 1);


---to get first 50 unpublished records from final_output
  WITH to_publish AS (
                        SELECT merchant, customer
                        FROM final_output
                        WHERE published IS NULL
                        ORDER BY ystarttime
                        LIMIT 50
                        FOR UPDATE SKIP LOCKED),
                check_count AS (
                    SELECT COUNT(*) AS cnt FROM to_publish
                ),
                update_rows AS (
                    UPDATE final_output
                    SET published = 'published'
                    WHERE (merchant, customer) IN (
                        SELECT merchant, customer FROM to_publish
                    )
                    AND EXISTS (
                        SELECT 1 FROM check_count WHERE cnt = 50
                    )
                    RETURNING *
                )
                SELECT * FROM update_rows;








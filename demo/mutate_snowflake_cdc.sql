-- Second set of upstream changes for the stream demo: one delete and one update.
-- Same as section [S6] of demo/snowsight_demo.sql.
DELETE FROM SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS WHERE CLAIM_ID = 5;

-- Claim 3 is seeded as DENIED; approve it and adjust the amount.
UPDATE SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
   SET CLAIM_STATUS = 'PAID',
       AMOUNT       = AMOUNT + 50.00,
       UPDATED_AT   = CURRENT_TIMESTAMP()
 WHERE CLAIM_ID = 3;

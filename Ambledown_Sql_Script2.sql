SELECT  CAST(0                                                                                          AS DECIMAL(38,10))                  AS ACCOUNT_BALANCE
     ,  LPAD(CAST(ad.POLICY_NUMBER                                                                      AS VARCHAR(25)),25, '0')            AS  ACCOUNT_NUMBER
     ,  CAST(ad.POLICY_STATUS                                                                           AS VARCHAR(6))                      AS SOURCE_ACCOUNT_STATUS_CODE
     ,  CAST(ad.POLICY_STATUS                                                                           AS VARCHAR(35))                     AS PRODUCT_STATUS
     ,  CAST(sp.SUB_PRODUCT_CODE                                                                        AS VARCHAR(6))                      AS SOURCE_ACCOUNT_TYPE_CODE
     ,  CAST(ad.INFORMATION_DATE                                                                        AS DATE)                            AS INFORMATION_DATE
     ,  CAST(ad.INCEPTION_DATE                                                                          AS DATE)                            AS OPEN_DATE
     ,  CAST(ad.SITE_CODE                                                                               AS VARCHAR(10))                     AS SITE_CODE
     ,  CAST('AMBL'                                                                                     AS VARCHAR(6))                      AS PRODUCT_CODE
     ,  CAST('AMBL'                                                                                     AS VARCHAR(6))                      AS SOURCE_CODE
     ,  CAST('24AMBL'                                                                                   AS VARCHAR(6))                      AS ACCOUNT_TYPE_CODE
     ,  CAST('UMA AMBLEDOWN'                                                                            AS VARCHAR(35))                     AS PRODUCT
     ,  CAST(ad.SOURCE_DATA                                                                             AS VARCHAR(35))                     AS SOURCE_SYSTEM
     ,  CAST('UMA - AMBLEDOWN'                                                                          AS VARCHAR(35))                     AS SOURCE
     ,  CAST(RTRIM(LTRIM(REPLACE(ad.SUB_PRODUCT_CODE, ' ', '')))                                        AS VARCHAR(6))                      AS SOURCE_SUB_PRODUCT_CODE
     ,  TO_DATE(ad.COVER_EFFECTIVE_TO_DATE)                                                                                                 AS COVER_EFFECTIVE_TO_DATE
     ,  CAST(''                                                                                         AS DATE)                            AS MATURITY_DATE
     ,  CAST(''                                                                                         AS VARCHAR (35))                    AS PRODUCT_SUB_STATUS
     ,  CAST(0                                                                                          AS DECIMAL(38,10))                   AS PRE_PAYMENT_AMOUNT
     ,  CAST(0                                                                                          AS BIGINT)                           AS OVERDRAFT_INDICATOR
     ,  CAST(''                                                                                         AS VARCHAR(150))                    AS COLLECTIONS_STATUS
     ,  CAST(''                                                                                         AS VARCHAR(10))                      AS COLLECTIONS_STATUS_CODE
     ,  TO_DATE(ad.LAPSED_DATE)                                                                                                             AS LAPSED_DATE  
     ,  TO_DATE(ad.CANCELLED_DATE )                                                                                                         AS CANCELLED_DATE
     ,  CAST(ad.total_premium_amt                                                                       AS DECIMAL(38,18))                  AS MONTHLY_PREMIUM 
     ,  CAST(ad.total_sum_insured                                                                       AS DECIMAL(38,18))                  AS INSURED_COVER_AMOUNT 
     ,  CAST(''                                                                                         AS VARCHAR(20))                     AS SINGLE_UNIQUE_CUSTOMER_KEY
     ,  CAST(aas.ACCOUNT_STATUS_CODE                                                                    AS VARCHAR(6))                      AS ACCOUNT_STATUS_CODE
     ,  CAST(aas.ACCOUNT_STATUS                                                                         AS VARCHAR(35))                     AS ACCOUNT_STATUS
     ,  CAST(site.unitname                                                                              AS VARCHAR(35))                     AS SITE
     ,  CAST(prod.ProductDescription                                                                    AS VARCHAR(5))                      AS SUB_PRODUCT
     ,  CAST(prod.ProductDescription                                                                    AS VARCHAR(35))                     AS ACCOUNT_TYPE
     ,  CAST(sp.SUB_PRODUCT_CODE                                                                        AS VARCHAR(6))                      AS SUB_PRODUCT_CODE 
     ,  CAST(ad.CUSTOMER_KEY                                                                            AS VARCHAR(20))                     AS SOURCE_CUSTOMER_KEY
     ,  CONCAT(RPAD(cisa521.CLCAA521, 7 , ''), LPAD(CAST(cisa521.CLCAA521 AS VARCHAR(10)),3,'0'))                                            AS CIF_CUSTOMER_KEY
     ,  CAST(CASE
                WHEN CONCAT(RPAD(cisa521.clcaa521, 7 , ' '), LPAD(CAST(cisa521.clcsa521 AS VARCHAR(3)),3,'0'))  IS NULL
                    THEN ad.CUSTOMER_KEY
                ELSE NULL
             END                                                                                        AS VARCHAR(20))             AS NON_CIF_CUSTOMER_KEY
     ,  CAST(cust.idnumber                                                                              AS VARCHAR(20))                     AS ID_NUMBER                                                                                    
     ,  CAST(ad.COVER_EFFECTIVE_FROM_DATE                                                               AS DATE)                            AS COVER_EFFECTIVE_FROM_DATE
     ,  CAST(cust.companyregnumber                                                                      AS VARCHAR(20))                     AS COMPANY_REGISTRATION_NUMBER
     ,  CAST(cust.passportnumber                                                                        AS VARCHAR(20))                     AS PASSPORT_NUMBER

    FROM     parquet.`hdfs:///bigdatahdfs/project/sti/uma/cvm/za/AICAMBLEDOWNPOLICY/EXTRACT_DATE=#value1#/VERSION=1`    AS ad 
       
LEFT JOIN   parquet.`hdfs:///bigdatahdfs/project/sti/uma/cvm/za/AICMASTERSITE/EXTRACT_DATE=#value1#/VERSION=1` AS site
    ON          ad.SOURCE_DATA = site.System    
LEFT JOIN   parquet.`hdfs:///bigdatahdfs/project/sti/uma/cvm/za/AICPRODUCT/EXTRACT_DATE=#value1#/VERSION=1` AS prod
    ON          ad.SUB_PRODUCT_CODE = prod.SubProduct  
LEFT JOIN   parquet.`hdfs:///bigdatahdfs/project/sti/uma/cvm/za/AICCUSTOMER/EXTRACT_DATE=#value1#/VERSION=1` AS cust
    ON          ad.customer_key = cust.customerkey
LEFT JOIN   parquet.`hdfs:///bigdatahdfs/datalake/publish/cif/CISA522/enceladus_info_date=#value1#/enceladus_info_version=1` AS cisa521
    ON          cust.idnumber = cisa521.IDNOA521
    
LEFT JOIN  (SELECT _c1 AS SOURCE_SUB_PRODUCT_CODE, _c3 AS SUB_PRODUCT_CODE
              FROM  csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/Ambledown/MappingProd/Mapping_AMBL_Prod_Sprod_GEN.csv`) AS sp
    ON          ad.SUB_PRODUCT_CODE = sp.SOURCE_SUB_PRODUCT_CODE    
LEFT JOIN  (SELECT  _c0 AS SOURCE_ACCOUNT_STATUS_CODE, _c1 as ACCOUNT_STATUS_CODE, _c3 as ACCOUNT_STATUS
              FROM  csv.`hdfs:///bigdatahdfs/project/cvm/Staging/vLOOKUP/Ambledown/LookupAmbledownAccStatus/LOOKUP_AMBLEDOWN_ACCOUNT_STATUS.csv`) AS aas                           
    ON          ad.POLICY_STATUS = aas.source_account_status_code
   WHERE    ad.POLICY_STATUS NOT IN ('DRAFT','NTU','NTUC','NTUE')
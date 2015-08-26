-- Section 1: Set search path as needed
SET search_path TO analytics_dev,
       analytics,
       '$user',
       public;

-- Section 2: Store result names into a TEMPORARY table
DROP TABLE if exists ai_ppdm_acr_result_names;


SELECT DISTINCT test_result_name_stnd INTO temp TABLE ai_ppdm_acr_result_names
FROM analytics.lab_result
WHERE test_result_name_stnd LIKE 'IGF-I';


-- Section 3: Extract lab result records of interest
--        , add utilitarian fields: record numbers (unpartitioned, partitioned by patient asc, desc)
--        , Handle any required data cleansing
--        , add placeholder flags

DROP TABLE IF EXISTS ai_ppdm_acr;

CREATE temp TABLE ai_ppdm_acr
AS
SELECT     ROW_NUMBER() OVER (ORDER BY token4,test_observation_reported_date,lab_result_id) AS record_number,
    DENSE_RANK() OVER (PARTITION BY token4 ORDER BY test_observation_reported_date ASC,lr.test_result_name_stnd,lab_result_id) AS record_number_asc,
    DENSE_RANK() OVER (PARTITION BY token4 ORDER BY test_observation_reported_date DESC,lr.test_result_name_stnd,lab_result_id) AS record_number_desc,

   lab_result_id,
    input_filename,
    token4 AS patient_id,
    md5(test_observation_reported_date||token4) patient_date_id,
    patient_gender,
    patient_date_of_birth as patient_birth_year,
    CASE WHEN left(test_observation_reported_date,4) = '' then null
    ELSE left(test_observation_reported_date,4)::numeric - patient_date_of_birth::numeric END as patient_age,
    patient_state,
    lr.test_result_name_stnd,
    test_result_name,
    CASE WHEN test_result_value_numeric_stnd is null and test_result_value like '%<%' or test_result_value like '%>%' then replace(case WHEN test_result_value ~* '< ?[0-9]+ ?not detected' then '0' ELSE regexp_substr(test_result_value,'[0-9]+(\.[0-9]+)? ?') end,',','') *1.00
         WHEN test_result_value_numeric_stnd is null and test_result_value like '%E6' then replace(test_result_value,'E6','')*1000000.00
         WHEN test_result_value_numeric_stnd is null and test_result_value like '%E5' then replace(test_result_value,'E5','')*100000.00
         WHEN test_result_value_numeric_stnd is null and test_result_value like '%E4' then replace(test_result_value,'E4','')*10000.00
         WHEN test_result_value_numeric_stnd is null and test_result_value ~* 'not detected' then 0.00
         WHEN test_result_value_numeric_stnd is null and test_result_value ~* 'detected' then 1.00
    ELSE test_result_value_numeric_stnd end test_result_num,
    test_result_value,
    test_result_abnormal_flag as test_abnorm_flag,
    TO_DATE(CASE WHEN test_observation_reported_date = '' THEN test_specimen_receipt_date ELSE test_observation_reported_date END,'YYYYMMDD') test_observation_reported_date_dt,
    LEFT (CASE WHEN test_observation_reported_date = '' THEN test_specimen_receipt_date ELSE test_observation_reported_date END,6)*1 test_observation_reported_date_yyyymm,
    TO_DATE(test_specimen_draw_date,'YYYYMMDD') test_specimen_draw_date_dt,
    md5(coalesce(ordering_provider_npi_number,'')|| coalesce(ordering_provider_first_name,'')||coalesce(ordering_provider_last_name,'')) provider_id,
    ordering_practice_lab_account_number practice_id,
    CASE
         WHEN lower(account_type) IN ('other','jail','military','n/a',' ','deleted account') THEN 'Non-Reference'
         WHEN account_type is null THEN 'Non-Reference'
         WHEN lower(account_type) IN ('lab') THEN 'Reference'
         WHEN lower(account_type) IN ('foreign') THEN 'Non-USA'
         ELSE 'UNMAPPED' ||account_type
       END account_type_val,
    provider_type,
    diag_1 as icd9_primary,
    diag_2 as icd9_secondary,
    diag_3 as icd9_third,
    diag_4 as icd9_fourth,
    ordering_practice_address_line_1 IS NOT NULL has_practice_info,
    ((ordering_provider_first_name IS NOT NULL AND ordering_provider_last_name IS NOT NULL) OR ordering_provider_npi_number IS NOT NULL) has_provider_info,
    ordering_provider_npi_number <> '' has_npi,
    0 has_igf1_test,
    0 has_abn_igf1_test,
    0 has_acr_icd9,
    0 has_pituitary_neo_icd9,
    0 has_had_pituitary_neo_icd9,
    0 has_abn_igf1_and_pituitary_neo_icd9,
    0 has_diag
FROM analytics.lab_result lr
  INNER JOIN ai_ppdm_acr_result_names rn ON rn.test_result_name_stnd = lr.test_result_name_stnd;


-- Section 4: Populate flags accordingly

ALTER TABLE ai_ppdm_acr ADD COLUMN age_bucket varchar(6);

UPDATE ai_ppdm_acr
   SET age_bucket=(
    CASE
        WHEN patient_age < 1 THEN '<1'
        WHEN patient_age = 1 THEN '1'
        WHEN patient_age = 2 THEN '2'
        WHEN patient_age = 3 THEN '3'
        WHEN patient_age = 4 THEN '4'
        WHEN patient_age = 5 THEN '5'
        WHEN patient_age = 6 THEN '6'
        WHEN patient_age = 7 THEN '7'
        WHEN patient_age = 8 THEN '8'
        WHEN patient_age = 9 THEN '9'
        WHEN patient_age = 10 THEN '10'
        WHEN patient_age = 11 THEN '11'
        WHEN patient_age = 12 THEN '12'
        WHEN patient_age = 13 THEN '13'
        WHEN patient_age = 14 THEN '14'
        WHEN patient_age = 15 THEN '15'
        WHEN patient_age = 16 THEN '16'
        WHEN patient_age = 17 THEN '17'
        WHEN patient_age = 18 THEN '18'
        WHEN patient_age = 19 THEN '19'
        WHEN patient_age = 20 THEN '20'
        WHEN patient_age BETWEEN 21 AND 25 THEN '21-25'
        WHEN patient_age BETWEEN 26 AND 30 THEN '26-30'
        WHEN patient_age BETWEEN 31 AND 35 THEN '31-35'
        WHEN patient_age BETWEEN 36 AND 40 THEN '36-40'
        WHEN patient_age BETWEEN 41 AND 45 THEN '41-45'
        WHEN patient_age BETWEEN 46 AND 50 THEN '46-50'
        WHEN patient_age BETWEEN 51 AND 55 THEN '51-55'
        WHEN patient_age BETWEEN 56 AND 60 THEN '56-60'
        WHEN patient_age BETWEEN 61 AND 65 THEN '61-65'
        WHEN patient_age BETWEEN 66 AND 70 THEN '66-70'
        WHEN patient_age BETWEEN 71 AND 75 THEN '71-75'
        WHEN patient_age BETWEEN 76 AND 80 THEN '76-80'
        WHEN patient_age BETWEEN 81 AND 85 THEN '81-85'
        WHEN patient_age BETWEEN 86 AND 90 THEN '86-90'
        WHEN patient_age > 90  THEN '>90'
        ELSE NULL
    END),
    has_igf1_test = (test_result_name_stnd = 'IGF-I'),
    has_acr_icd9 = case when '253.0' in (icd9_primary,icd9_secondary,icd9_third,icd9_fourth) then 1 else 0 end,
    has_pituitary_neo_icd9 = case when '227.3' in (icd9_primary,icd9_secondary,icd9_third,icd9_fourth) then 1 else 0 end;


UPDATE ai_ppdm_acr
SET has_abn_igf1_test = (
    CASE
        WHEN (age_bucket ='<1' AND patient_gender = 'M' AND test_result_num >157) OR (age_bucket ='<1' AND patient_gender = 'F' AND test_result_num >126) THEN 1
        WHEN (age_bucket ='1' AND patient_gender = 'M' AND test_result_num >167) OR (age_bucket ='1' AND patient_gender = 'F' AND test_result_num >132) THEN 1
        WHEN (age_bucket ='2' AND patient_gender = 'M' AND test_result_num >184) OR (age_bucket ='2' AND patient_gender = 'F' AND test_result_num >145) THEN 1
        WHEN (age_bucket ='3' AND patient_gender = 'M' AND test_result_num >205) OR (age_bucket ='3' AND patient_gender = 'F' AND test_result_num >164) THEN 1
        WHEN (age_bucket ='4' AND patient_gender = 'M' AND test_result_num >225) OR (age_bucket ='4' AND patient_gender = 'F' AND test_result_num >188) THEN 1
        WHEN (age_bucket ='5' AND patient_gender = 'M' AND test_result_num >246) OR (age_bucket ='5' AND patient_gender = 'F' AND test_result_num >214) THEN 1
        WHEN (age_bucket ='6' AND patient_gender = 'M' AND test_result_num >267) OR (age_bucket ='6' AND patient_gender = 'F' AND test_result_num >240) THEN 1
        WHEN (age_bucket ='7' AND patient_gender = 'M' AND test_result_num >292) OR (age_bucket ='7' AND patient_gender = 'F' AND test_result_num >267) THEN 1
        WHEN (age_bucket ='8' AND patient_gender = 'M' AND test_result_num >323) OR (age_bucket ='8' AND patient_gender = 'F' AND test_result_num >305) THEN 1
        WHEN (age_bucket ='9' AND patient_gender = 'M' AND test_result_num >362) OR (age_bucket ='9' AND patient_gender = 'F' AND test_result_num >349) THEN 1
        WHEN (age_bucket ='10' AND patient_gender = 'M' AND test_result_num >407) OR (age_bucket ='10' AND patient_gender = 'F' AND test_result_num >400) THEN 1
        WHEN (age_bucket ='11' AND patient_gender = 'M' AND test_result_num >454) OR (age_bucket ='11' AND patient_gender = 'F' AND test_result_num >453) THEN 1
        WHEN (age_bucket ='12' AND patient_gender = 'M' AND test_result_num >499) OR (age_bucket ='12' AND patient_gender = 'F' AND test_result_num >499) THEN 1
        WHEN (age_bucket ='13' AND patient_gender = 'M' AND test_result_num >533) OR (age_bucket ='13' AND patient_gender = 'F' AND test_result_num >533) THEN 1
        WHEN (age_bucket ='14' AND patient_gender = 'M' AND test_result_num >551) OR (age_bucket ='14' AND patient_gender = 'F' AND test_result_num >552) THEN 1
        WHEN (age_bucket ='15' AND patient_gender = 'M' AND test_result_num >554) OR (age_bucket ='15' AND patient_gender = 'F' AND test_result_num >554) THEN 1
        WHEN (age_bucket ='16' AND patient_gender = 'M' AND test_result_num >542) OR (age_bucket ='16' AND patient_gender = 'F' AND test_result_num >542) THEN 1
        WHEN (age_bucket ='17' AND patient_gender = 'M' AND test_result_num >521) OR (age_bucket ='17' AND patient_gender = 'F' AND test_result_num >517) THEN 1
        WHEN (age_bucket ='18' AND patient_gender = 'M' AND test_result_num >494) OR (age_bucket ='18' AND patient_gender = 'F' AND test_result_num >486) THEN 1
        WHEN (age_bucket ='19' AND patient_gender = 'M' AND test_result_num >463) OR (age_bucket ='19' AND patient_gender = 'F' AND test_result_num >451) THEN 1
        WHEN (age_bucket ='20' AND patient_gender = 'M' AND test_result_num >430) OR (age_bucket ='20' AND patient_gender = 'F' AND test_result_num >416) THEN 1
        WHEN (age_bucket ='21-25' AND patient_gender = 'M' AND test_result_num >355) OR (age_bucket ='21-25' AND patient_gender = 'F' AND test_result_num >342) THEN 1
        WHEN (age_bucket ='26-30' AND patient_gender = 'M' AND test_result_num >282) OR (age_bucket ='26-30' AND patient_gender = 'F' AND test_result_num >270) THEN 1
        WHEN (age_bucket ='31-35' AND patient_gender = 'M' AND test_result_num >246) OR (age_bucket ='31-35' AND patient_gender = 'F' AND test_result_num >243) THEN 1
        WHEN (age_bucket ='36-40' AND patient_gender = 'M' AND test_result_num >233) OR (age_bucket ='36-40' AND patient_gender = 'F' AND test_result_num >227) THEN 1
        WHEN (age_bucket ='41-45' AND patient_gender = 'M' AND test_result_num >216) OR (age_bucket ='41-45' AND patient_gender = 'F' AND test_result_num >204) THEN 1
        WHEN (age_bucket ='46-50' AND patient_gender = 'M' AND test_result_num >205) OR (age_bucket ='46-50' AND patient_gender = 'F' AND test_result_num >195) THEN 1
        WHEN (age_bucket ='51-55' AND patient_gender = 'M' AND test_result_num >200) OR (age_bucket ='51-55' AND patient_gender = 'F' AND test_result_num >190) THEN 1
        WHEN (age_bucket ='56-60' AND patient_gender = 'M' AND test_result_num >194) OR (age_bucket ='56-60' AND patient_gender = 'F' AND test_result_num >172) THEN 1
        WHEN (age_bucket ='61-65' AND patient_gender = 'M' AND test_result_num >188) OR (age_bucket ='61-65' AND patient_gender = 'F' AND test_result_num >169) THEN 1
        WHEN (age_bucket ='66-70' AND patient_gender = 'M' AND test_result_num >192) OR (age_bucket ='66-70' AND patient_gender = 'F' AND test_result_num >163) THEN 1
        WHEN (age_bucket ='71-75' AND patient_gender = 'M' AND test_result_num >179) OR (age_bucket ='71-75' AND patient_gender = 'F' AND test_result_num >165) THEN 1
        WHEN (age_bucket ='76-80' AND patient_gender = 'M' AND test_result_num >172) OR (age_bucket ='76-80' AND patient_gender = 'F' AND test_result_num >165) THEN 1
        WHEN (age_bucket ='81-85' AND patient_gender = 'M' AND test_result_num >165) OR (age_bucket ='81-85' AND patient_gender = 'F' AND test_result_num >172) THEN 1
        WHEN (age_bucket ='86-90' AND patient_gender = 'M' AND test_result_num >166) OR (age_bucket ='86-90' AND patient_gender = 'F' AND test_result_num >178) THEN 1
        WHEN (age_bucket ='>90' AND patient_gender = 'M' AND test_result_num >166) OR (age_bucket ='>90' AND patient_gender = 'F' AND test_result_num >178) THEN 1
    ELSE 0
    END);

UPDATE ai_ppdm_acr
SET has_had_pituitary_neo_icd9 = (select max(has_pituitary_neo_icd9)
                                  FROM ai_ppdm_acr h2
                                  WHERE h2.patient_id = ai_ppdm_acr.patient_id
                                        AND   h2.test_observation_reported_date_dt <= ai_ppdm_acr.test_observation_reported_date_dt);


UPDATE ai_ppdm_acr
SET has_diag = has_abn_igf1_test= 1 OR has_acr_icd9 = 1,
    has_abn_igf1_and_pituitary_neo_icd9 = has_abn_igf1_test= 1 AND has_had_pituitary_neo_icd9 = 1;

ALTER TABLE ai_ppdm_acr ADD COLUMN patients_with_abn_igf1_test_and_acr_icd9 INT;
ALTER TABLE ai_ppdm_acr ADD COLUMN patients_with_abn_igf1_test_and_not_acr_icd9 INT;
ALTER TABLE ai_ppdm_acr ADD COLUMN patients_with_not_abn_igf1_test_and_acr_icd9 INT;
ALTER TABLE ai_ppdm_acr ADD COLUMN patients_with_not_abn_igf1_test_and_pituitary_neo_icd9 INT;

UPDATE ai_ppdm_acr
SET patients_with_abn_igf1_test_and_acr_icd9=1 WHERE (has_abn_igf1_test = 1 AND has_acr_icd9 = 1 );
	
UPDATE ai_ppdm_acr
SET patients_with_abn_igf1_test_and_not_acr_icd9=1 WHERE (has_abn_igf1_test = 1 AND has_acr_icd9 = 0);

UPDATE ai_ppdm_acr
SET patients_with_not_abn_igf1_test_and_acr_icd9=1 WHERE (has_abn_igf1_test = 0 AND has_acr_icd9 = 1);

UPDATE ai_ppdm_acr
SET patients_with_not_abn_igf1_test_and_pituitary_neo_icd9=1 WHERE (has_abn_igf1_test = 0 AND has_pituitary_neo_icd9 = 1);
	


-- Section 5: QA
/*
SELECT test_observation_reported_date_yyyymm,
       COUNT(*) total_number_of_records,
       SUM(has_diag) has_diag,
       SUM(has_igf1_test) has_igf1_test,
       sum(has_abn_igf1_test) has_abn_igf1_test,
       sum(has_acr_icd9) has_acr_icd9,
       sum(has_pituitary_neo_icd9) has_pituitary_neo_icd9,
       sum(has_had_pituitary_neo_icd9) has_had_pituitary_neo_icd9,
       sum(has_abn_igf1_and_pituitary_neo_icd9) has_abn_igf1_and_pituitary_neo_icd9
FROM ai_ppdm_acr
GROUP BY test_observation_reported_date_yyyymm;
*/




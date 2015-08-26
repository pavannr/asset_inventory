-- Section 1: Set search path as needed
SET search_path to analytics_dev,'$user',analytics,public;

-- Section 2: Store result names into a TEMPORARY table
DROP TABLE if exists ai_ppdm_hcv_result_names;

SELECT DISTINCT test_result_name_stnd INTO temp TABLE ai_ppdm_hcv_result_names
FROM analytics.lab_result
WHERE test_result_name_stnd LIKE '%HCV%';





-- Section 3: Extract lab result records of interest
--        , add utilitarian fields: record numbers (unpartitioned, partitioned by patient asc, desc)
--        , Handle any required data cleansing
--        , add placeholder flags


DROP TABLE IF EXISTS ai_ppdm_hcv;
CREATE TABLE ai_ppdm_hcv AS
SELECT
		ROW_NUMBER() OVER (ORDER BY token4,test_observation_reported_date,lab_result_id) AS record_number,
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
    CASE
         WHEN lr.test_result_name_stnd IN ('HCV PCR QUANT','HCV RNA QUANT') THEN 'HCV VL'
         ELSE lr.test_result_name_stnd
    END result_type,
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
       0 has_diag,
       0 has_fibrosure_test,
       0 has_gt1,
       0 has_gt1a,
       0 has_gt1b,
       0 has_ab_test,
       0 has_ab_gt8_test,
       0 has_gt_test,
       0 has_pcr_quant_test,
       0 has_rna_quant_test,
       0 has_rna_qual_test,
       0 has_vl_test,
       0 has_vl_quant_test,
       0 has_vl_quant_gt43_test,
       0 has_vl_quant_lt43_test,
       0 has_vl_qual_test,
       0 has_vl_qual_pos_test,
       0 has_vl_qual_neg_test,
       0 tests_prior_12mo_gt,
       0 tests_prior_12mo_vl,
       0 tests_prior_gt,
       0 tests_prior_vl,
       0 is_in_treatment,
       0 is_warehoused,
       0 is_in_treatment_not_at_goal,
       0 is_in_treatment_success,
       0 is_in_treatment_pending_lost
FROM analytics.lab_result lr
  INNER JOIN ai_ppdm_hcv_result_names rn ON rn.test_result_name_stnd = lr.test_result_name_stnd;


-- Section 4: Populate flags accordingly

UPDATE ai_ppdm_hcv
   SET has_ab_test = (test_result_name_stnd = 'HCV AB'),
       has_ab_gt8_test = (test_result_name_stnd = 'HCV AB' AND (test_result_value ~* '^reactive' OR test_result_num >= 8)),
       has_pcr_quant_test = (test_result_name_stnd IN ('HCV PCR QUANT')),
       has_rna_quant_test = (test_result_name_stnd IN ('HCV RNA QUANT')),
       has_rna_qual_test = (test_result_name_stnd IN ('HCV RNA QUAL')),
       has_vl_quant_test = (test_result_name_stnd IN ('HCV PCR QUANT','HCV RNA QUANT')),
       has_vl_quant_gt43_test = (test_result_name_stnd IN ('HCV PCR QUANT','HCV RNA QUANT') AND test_result_num >= 43),
       has_vl_quant_lt43_test = (test_result_name_stnd IN ('HCV PCR QUANT','HCV RNA QUANT') AND test_result_num < 43),
       has_vl_qual_test = (test_result_name_stnd = 'HCV RNA QUAL'),
       has_vl_qual_pos_test = (test_result_name_stnd IN ('HCV RNA QUAL') AND test_result_value ~* '^detected$'),
       has_vl_qual_neg_test = (test_result_name_stnd IN ('HCV RNA QUAL') AND test_result_value ~* '^not detected$'),
       has_gt_test = (test_result_name_stnd = 'HCV GENOTYPE'),
       has_fibrosure_test = (test_result_name_stnd = 'HCV FIBROSURE'),
       has_gt1 = (test_result_name_stnd = 'HCV GENOTYPE' AND test_result_value ~* '^(genotype )?1[ab]?$'),
       has_gt1a = (test_result_name_stnd = 'HCV GENOTYPE' AND test_result_value ~* '^(genotype )?1a$'),
       has_gt1b = (test_result_name_stnd = 'HCV GENOTYPE' AND test_result_value ~* '^(genotype )?1b$');

UPDATE ai_ppdm_hcv
   SET has_diag = GREATEST(has_ab_gt8_test,has_gt_test,has_vl_quant_test,has_vl_qual_test)
            , has_vl_test= (has_vl_qual_test=1 OR has_vl_quant_test= 1);

UPDATE ai_ppdm_hcv
   SET tests_prior_gt = (SELECT COUNT(DISTINCT test_observation_reported_date_dt)
                         FROM ai_ppdm_hcv h2
                         WHERE h2.patient_id = ai_ppdm_hcv.patient_id
                         AND   h2.test_observation_reported_date_dt < ai_ppdm_hcv.test_observation_reported_date_dt
                         AND   h2.result_type = ai_ppdm_hcv.result_type)
WHERE result_type = 'HCV GENOTYPE';

UPDATE ai_ppdm_hcv
   SET tests_prior_12mo_gt = (SELECT COUNT(DISTINCT test_observation_reported_date_dt)
                              FROM ai_ppdm_hcv h2
                              WHERE h2.patient_id = ai_ppdm_hcv.patient_id
                              AND   h2.test_observation_reported_date_dt < ai_ppdm_hcv.test_observation_reported_date_dt
                              AND   h2.test_observation_reported_date_yyyymm >= (ai_ppdm_hcv.test_observation_reported_date_yyyymm -100)
                              AND   h2.result_type = ai_ppdm_hcv.result_type)
WHERE result_type = 'HCV GENOTYPE';

UPDATE ai_ppdm_hcv
   SET tests_prior_vl = (SELECT COUNT(DISTINCT test_observation_reported_date_dt)
                         FROM ai_ppdm_hcv h2
                         WHERE h2.patient_id = ai_ppdm_hcv.patient_id
                         AND   h2.test_observation_reported_date_dt < ai_ppdm_hcv.test_observation_reported_date_dt
                         AND   h2.result_type = ai_ppdm_hcv.result_type)
WHERE result_type = 'HCV VL';

UPDATE ai_ppdm_hcv
   SET tests_prior_12mo_vl = (SELECT COUNT(DISTINCT test_observation_reported_date_dt)
                              FROM ai_ppdm_hcv h2
                              WHERE h2.patient_id = ai_ppdm_hcv.patient_id
                              AND   h2.test_observation_reported_date_dt < ai_ppdm_hcv.test_observation_reported_date_dt
                              AND   h2.test_observation_reported_date_yyyymm >= (ai_ppdm_hcv.test_observation_reported_date_yyyymm -100)
                              AND   h2.result_type = ai_ppdm_hcv.result_type)
WHERE result_type = 'HCV VL';



UPDATE ai_ppdm_hcv
   SET is_in_treatment = ((tests_prior_12mo_vl+has_vl_test)>=2 OR ((tests_prior_12mo_vl+has_vl_test)=1 AND (tests_prior_12mo_gt+has_gt_test)>0));







UPDATE ai_ppdm_hcv
   SET is_in_treatment_not_at_goal = (has_diag=1 and is_in_treatment= 1 and (has_vl_qual_pos_test=1 OR has_vl_quant_gt43_test= 1))
   ,is_in_treatment_success = (has_diag=1 and is_in_treatment= 1 and (has_vl_qual_neg_test=1 OR has_vl_quant_lt43_test= 1));


UPDATE ai_ppdm_hcv
   SET is_in_treatment_pending_lost = (has_diag=1 and has_ab_test=1 AND (SELECT COUNT(DISTINCT test_observation_reported_date_dt)
                         FROM ai_ppdm_hcv h2
                         WHERE h2.patient_id = ai_ppdm_hcv.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hcv.test_observation_reported_date_dt
                         AND   (h2.has_gt_test = 1 OR   h2.has_vl_test = 1) )= 0);

UPDATE ai_ppdm_hcv
   SET is_warehoused = (( has_gt_test = 1 AND tests_prior_vl=0) OR (has_vl_test=1 AND tests_prior_12mo_vl between 1 and 2));


-- Section 5: QA
/*
select test_observation_reported_date_yyyymm,
count(*) total_number_of_records,
sum(has_diag) has_diag,
sum(has_fibrosure_test) has_fibrosure_test,
sum(has_gt1) has_gt1,
sum(has_gt1a) has_gt1a,
sum(has_gt1b) has_gt1b,
sum(has_ab_test) has_ab_test,
sum(has_ab_gt8_test) has_ab_gt8_test,
sum(has_gt_test) has_gt_test,
sum(has_pcr_quant_test) has_pcr_quant_test,
sum(has_rna_quant_test) has_rna_quant_test,
sum(has_rna_qual_test) has_rna_qual_test,
sum(has_vl_test) has_vl_test,
sum(has_vl_quant_test) has_vl_quant_test,
sum(has_vl_quant_gt43_test) has_vl_quant_gt43_test,
sum(has_vl_quant_lt43_test) has_vl_quant_lt43_test,
sum(has_vl_qual_test) has_vl_qual_test,
sum(has_vl_qual_pos_test) has_vl_qual_pos_test,
sum(has_vl_qual_neg_test) has_vl_qual_neg_test,
avg(tests_prior_12mo_gt) tests_prior_12mo_gt,
avg(tests_prior_12mo_vl) tests_prior_12mo_vl,
avg(tests_prior_gt) tests_prior_12mo_vl,
avg(tests_prior_vl) tests_prior_vl,
sum(is_in_treatment) is_in_treatment,
sum(is_warehoused) is_warehoused,
sum(is_in_treatment_not_at_goal) is_in_treatment_not_at_goal,
sum(is_in_treatment_success) is_in_treatment_success,
sum(is_in_treatment_pending_lost) is_in_treatment_pending_lost
from ai_ppdm_hcv
group by test_observation_reported_date_yyyymm;
*/



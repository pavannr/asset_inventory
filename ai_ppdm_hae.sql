SET search_path to analytics_dev,'$user',public;

DROP TABLE IF EXISTS ai_ppdm_hae_result_names;
		 
SELECT DISTINCT test_result_name_stnd INTO temp TABLE ai_ppdm_hae_result_names
FROM analytics.lab_result
WHERE test_result_name_stnd in ('COMPLEMENT C4, SERUM','C1 ESTERASE INHIBITOR, SERUM','C1 ESTERASE INHIBITOR, FUNC') OR test_result_name in ('C1 INHIBITOR, FUNCTIONAL');

		 
DROP TABLE IF EXISTS ai_ppdm_hae;

CREATE TABLE ai_ppdm_hae
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY token4,test_observation_reported_date,lab_result_id) AS record_number,
DENSE_RANK() OVER (PARTITION BY token4 ORDER BY test_observation_reported_date ASC,dd.test_result_name_stnd,lab_result_id) AS record_number_asc,
DENSE_RANK() OVER (PARTITION BY token4 ORDER BY test_observation_reported_date DESC,dd.test_result_name_stnd,lab_result_id) AS record_number_desc,
    dd.lab_result_id,
    input_filename,
    token4 AS patient_id,
    md5(test_observation_reported_date||token4) patient_date_id,
    patient_gender,
    patient_date_of_birth as patient_birth_year,
    CASE WHEN left(test_observation_reported_date,4) = '' then null
    ELSE left(test_observation_reported_date,4)::numeric - patient_date_of_birth::numeric END as patient_age,
    patient_state,
    dd.test_result_name_stnd,
    test_result_name,
    test_result_value_numeric_stnd as test_result_num,
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
    0 has_complement_c4_test,
    0 has_c1_serum_test,
    0 has_c1_func_test,
	0 complement_c4_between_16_and_47,
	0 c1_lt_21,
	0 c1_gte_21,
	0 c1_func_lte_67,
	0 c1_func_lte_41,
	0 c1_func_btw_41_and_67,
    0 complement_c4_between_16_and_47_prior,
    0 c1_lt_21_prior,
    0 c1_gte_21_prior,
    0 c1_func_lte_67_prior,
    0 c1_func_lte_41_prior,
    0 c1_func_btw_41_and_67_prior,	
  	0 has_diag,
    0 Consistent_to_HAE_patients,
    0 Equivocal_for_HAE_patients
FROM analytics.lab_result dd
  INNER JOIN ai_ppdm_hae_result_names lr ON dd.test_result_name_stnd = lr.test_result_name_stnd;

  
update ai_ppdm_hae set has_complement_c4_test =1 where(test_result_name_stnd = 'COMPLEMENT C4, SERUM');
update ai_ppdm_hae set has_c1_serum_test =1  where(test_result_name_stnd = 'C1 ESTERASE INHIBITOR, SERUM');
update ai_ppdm_hae set has_c1_func_test =1 where (test_result_name_stnd = 'C1 ESTERASE INHIBITOR, FUNC');
update ai_ppdm_hae set complement_c4_between_16_and_47= 1 where has_complement_c4_test = 1 and test_result_num between 16 and 47;
update ai_ppdm_hae set c1_lt_21=1 where has_c1_serum_test=1 and test_result_num <21;
update ai_ppdm_hae set c1_gte_21=1 where has_c1_serum_test=1 and test_result_num >=21;
update ai_ppdm_hae set c1_func_lte_67=1 where (has_c1_func_test=1 or test_result_name in ('C1 INHIBITOR, FUNCTIONAL')) and (test_result_num <= 67);
update ai_ppdm_hae set c1_func_lte_41=1 where (has_c1_func_test=1 or test_result_name in ('C1 INHIBITOR, FUNCTIONAL')) and (test_result_num < 41);
update ai_ppdm_hae set c1_func_btw_41_and_67=1 where (has_c1_func_test or test_result_name in ('C1 INHIBITOR, FUNCTIONAL')) and (test_result_num between 41 and 67);
	   


UPDATE ai_ppdm_hae
   SET
       complement_c4_between_16_and_47_prior = (SELECT max(complement_c4_between_16_and_47)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
	                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);

UPDATE ai_ppdm_hae
   SET
       c1_lt_21_prior = (SELECT max(c1_lt_21)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);
UPDATE ai_ppdm_hae
   SET
       c1_gte_21_prior = (SELECT max(c1_gte_21)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);

UPDATE ai_ppdm_hae
   SET
       c1_func_lte_67_prior = (SELECT max(c1_func_lte_67)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);
UPDATE ai_ppdm_hae
   SET
       c1_func_lte_41_prior = (SELECT max(c1_func_lte_41)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);
UPDATE ai_ppdm_hae
   SET
       c1_func_btw_41_and_67_prior = (SELECT max(c1_func_btw_41_and_67)
                         FROM ai_ppdm_hae h2
                         WHERE h2.patient_id = ai_ppdm_hae.patient_id
                         AND   h2.test_observation_reported_date_dt <= ai_ppdm_hae.test_observation_reported_date_dt);
UPDATE ai_ppdm_hae
   SET has_diag=1 where ((complement_c4_between_16_and_47_prior=1  AND c1_lt_21_prior=1) 
				OR (complement_c4_between_16_and_47_prior=1 AND c1_gte_21_prior=1 AND c1_func_lte_67_prior=1));
				

UPDATE ai_ppdm_hae
   SET Consistent_to_HAE_patients=((complement_c4_between_16_and_47_prior=1  AND c1_lt_21_prior=1) 
				OR (complement_c4_between_16_and_47_prior=1 AND c1_gte_21_prior=1 AND c1_func_lte_41_prior=1));


UPDATE ai_ppdm_hae
   SET Equivocal_for_HAE_patients=(complement_c4_between_16_and_47_prior=1 and c1_gte_21_prior=1 and c1_func_btw_41_and_67_prior=1);


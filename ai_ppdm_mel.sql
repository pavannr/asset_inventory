SET search_path to analytics_dev,'$user',public;

DROP TABLE IF EXISTS ai_ppdm_mel_result_names;

SELECT DISTINCT test_result_name_stnd INTO temp TABLE ai_ppdm_mel_result_names
FROM analytics.lab_result 
WHERE 
test_result_name_stnd in ('BRAF MUTATION ANALYSIS', 'BRAF MUTATION ANALYSIS, V600', 'BRAF MUTATION ANALYSIS V600')
OR test_result_name_stnd = 'SPECIMEN SOURCE';




DROP TABLE IF EXISTS ai_ppdm_mel1;

CREATE TEMPORARY TABLE ai_ppdm_mel1
AS
SELECT

ROW_NUMBER() OVER (ORDER BY token4,d.test_observation_reported_date,d.lab_result_id) AS record_number,
DENSE_RANK() OVER (PARTITION BY token4 ORDER BY d.test_observation_reported_date ASC,d.test_result_name_stnd,d.lab_result_id) AS record_number_asc,
DENSE_RANK() OVER (PARTITION BY token4 ORDER BY d.test_observation_reported_date DESC,d.test_result_name_stnd,d.lab_result_id) AS record_number_desc,
       d.lab_result_id,	
       input_filename,
       token4 AS patient_id,
       token14,
       md5(d.test_observation_reported_date||token4) patient_date_id,
       patient_gender,
       patient_date_of_birth AS patient_birth_year,
       CASE
       WHEN LEFT (d.test_observation_reported_date,4) = '' THEN NULL
       ELSE LEFT (d.test_observation_reported_date,4)::numeric- patient_date_of_birth::numeric
       END AS patient_age,
       patient_state,
       d.test_result_name,
       d.test_result_name_stnd,
       d.test_order_name,
       d.test_result_code,
       d.test_result_value_numeric_stnd test_result_num,
       d.test_result_value_stnd AS test_result,
       d.test_resulted_for_clinical,
       d.parent_test_order_name,
       d.panel_order_name,
       d.parent_panel_order_name,
       d.test_specimen_source_stnd,
       d.test_result_value as test_result_val,
       test_result_abnormal_flag AS test_abnorm_flag,
          TO_DATE(CASE WHEN d.test_observation_reported_date = '' THEN test_specimen_receipt_date ELSE d.test_observation_reported_date END,'YYYYMMDD') test_observation_reported_date_dt,
       LEFT (CASE WHEN d.test_observation_reported_date = '' THEN test_specimen_receipt_date ELSE d.test_observation_reported_date END,6)*1 test_observation_reported_date_yyyymm,
       TO_DATE(test_specimen_draw_date,'YYYYMMDD') test_specimen_draw_date_dt,
    patient_bill_type,
    md5(coalesce(ordering_provider_npi_number,'')|| coalesce(ordering_provider_first_name,'')||coalesce(ordering_provider_last_name,'')) provider_id,
    ordering_practice_lab_account_number practice_id,
    CASE
    WHEN lower(account_type) IN ('other','jail','military','n/a',' ','deleted account') THEN           'Non-Reference'
    WHEN account_type is null THEN 'Non-Reference'
    WHEN lower(account_type) IN ('lab') THEN 'Reference'
    WHEN lower(account_type) IN ('foreign') THEN 'Non-USA'
    ELSE 'UNMAPPED' ||account_type
    END account_type_val,
    provider_type,
diagnosis_codes,
    diag_1 as icd9_primary,
    diag_2 as icd9_secondary,
    diag_3 as icd9_third,
    diag_4 as icd9_fourth,
       0 specimen_source_result,
       0 specimen_source_result_skin,
       0 braf_result,
       0 braf_result_mel,
       0 braf_result_mel_detected,
       0 new_pats,
       0 has_braf_pstv_mutation,
    ordering_practice_address_line_1 IS NOT NULL has_practice_info,
    ((ordering_provider_first_name IS NOT NULL AND ordering_provider_last_name IS NOT NULL) OR ordering_provider_npi_number IS NOT NULL) has_provider_info,
    ordering_provider_npi_number <> '' has_npi,
       TO_DATE(d.test_observation_reported_date,'YYYYMMDD') test_observation_reported_date_last,
       TO_DATE(d.test_observation_reported_date,'YYYYMMDD') test_observation_reported_date_first
from analytics.lab_result d
 INNER JOIN ai_ppdm_mel_result_names dd ON dd.test_result_name_stnd = d.test_result_name_stnd;


DROP TABLE IF EXISTS ai_ppdm_mel;

CREATE TABLE ai_ppdm_mel
AS
SELECT b.*,
       c.lab_result_id AS lab_result_id_prior,
       c.test_observation_reported_date_dt AS test_observation_reported_date_dt_prior,
       c.test_result_num AS test_result_num_prior,
       b.test_result_num AS test_result_num_last,
       b.test_result_num AS test_result_num_first,
       DATEDIFF('day',b.test_observation_reported_date_dt,c.test_observation_reported_date_dt)       days_since_last_test
FROM ai_ppdm_mel1 b
  LEFT JOIN ai_ppdm_mel1 c
         ON c.patient_id = b.patient_id
        AND c.record_number = b.record_number -1;





UPDATE ai_ppdm_mel
SET 
specimen_source_result = test_result_name_stnd in ('SPECIMEN SOURCE'),
braf_result  = test_result_name_stnd in ('BRAF MUTATION ANALYSIS', 'BRAF MUTATION ANALYSIS, V600', 'BRAF MUTATION ANALYSIS V600');



UPDATE ai_ppdm_mel
SET 
specimen_source_result_skin = (specimen_source_result = 1 AND test_result like ('%Skin%'));

UPDATE ai_ppdm_mel
SET
braf_result_mel  = 
(braf_result = 1 AND 
(test_resulted_for_clinical = 'Yes' AND 
(test_order_name like ('%Melanoma%') OR test_result_name  like ('%Melanoma%') OR parent_test_order_name  like ('%Melanoma%')
OR panel_order_name  like ('%Melanoma%') OR parent_panel_order_name =  ('%Melanoma%') OR test_specimen_source_stnd like ('%Melanoma%') 
OR icd9_primary like ('%172%') 
OR icd9_secondary like ('%172%') 
OR icd9_third like ('%172%') 
OR icd9_fourth like ('%172%')
OR token14 in (select distinct token14 from ai_ppdm_mel where specimen_source_result_skin = 1))));


UPDATE ai_ppdm_mel
SET
braf_result_mel_detected = (braf_result_mel = 1 and test_result in('DETECTED','POSITIVE')),
new_pats = (braf_result_mel = 1);


UPDATE ai_ppdm_mel
SET
has_braf_pstv_mutation = (braf_result_mel_detected = 1 and braf_result_mel = 1);




-------------------------------------------------------------------------------------------------------------------------




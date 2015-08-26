
SET search_path TO analytics_dev,
       analytics,
       '$user',
       public;

-- Section 2a: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - ALL TIME
DROP TABLE IF EXISTS practice_provider_all_hist;

SELECT account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_all_hist
FROM ai_ppdm_hcv
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
GROUP BY account_type_val,
         provider_type;

-- Section 2b: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - LAST YEAR

DROP TABLE IF EXISTS practice_provider_all_ly;

SELECT account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_all_ly
FROM ai_ppdm_hcv
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv) -100
GROUP BY account_type_val,
         provider_type;

-- Section 3a: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - HAS PRACTICE INFO - ALL TIME

DROP TABLE IF EXISTS practice_provider_map2prac_hist;

SELECT account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_map2prac_hist
FROM ai_ppdm_hcv
WHERE has_practice_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
GROUP BY account_type_val,
         provider_type;

-- Section 3a: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - HAS PRACTICE INFO - LAST YEAR

DROP TABLE IF EXISTS practice_provider_map2prac_ly;

SELECT account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_map2prac_ly
FROM ai_ppdm_hcv
WHERE has_practice_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv) -100
GROUP BY account_type_val,
         provider_type;

-- Section 4a: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - HAS PROVIDER INFO - ALL TIME

DROP TABLE IF EXISTS practice_provider_map2prov_hist;

SELECT account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_map2prov_hist
FROM ai_ppdm_hcv
WHERE has_provider_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
GROUP BY account_type_val,
         provider_type;

-- Section 4b: Generate counts OF DISTINCT PATIENTS, PRACTICES AND PROVIDERS - HAS PROVIDER INFO - LAST YEAR

DROP TABLE IF EXISTS practice_provider_map2prov_ly;

SELECT account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost INTO temp TABLE practice_provider_map2prov_ly
FROM ai_ppdm_hcv
WHERE has_provider_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv) -100
GROUP BY account_type_val,
         provider_type;
         



DROP TABLE IF EXISTS practice_provider_map2NPI_hist;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost
	   into temp table practice_provider_map2NPI_hist
FROM ai_ppdm_hcv
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
GROUP BY 
       account_type_val,
        provider_type;
	 
	

DROP TABLE IF EXISTS practice_provider_map2NPI_ly;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_gt_test = 1 THEN patient_id END) patients_with_gt_test,
       COUNT(DISTINCT CASE WHEN has_gt1 = 1 THEN patient_id END) patients_with_gt1_test,
       COUNT(DISTINCT CASE WHEN has_gt1a = 1 THEN patient_id END) patients_with_gt1a_test,
       COUNT(DISTINCT CASE WHEN has_gt1b = 1 THEN patient_id END) patients_with_gt1b_test,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_with_diag,
       COUNT(DISTINCT CASE WHEN is_warehoused = 1 THEN patient_id END) patients_is_warehoused,
       COUNT(DISTINCT CASE WHEN is_in_treatment_not_at_goal = 1 THEN patient_id END) patients_is_in_treatment_not_at_goal,
       COUNT(DISTINCT CASE WHEN is_in_treatment_success = 1 THEN patient_id END) patients_is_in_treatment_success,
       COUNT(DISTINCT CASE WHEN is_in_treatment_pending_lost = 1 THEN patient_id END) patients_is_in_treatment_pending_lost
	   into temp table practice_provider_map2NPI_ly
FROM ai_ppdm_hcv
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv) -100
GROUP BY 
        account_type_val,
         provider_type;










-- Section 5: Pivot pre-calculated numbers into desired output table format (condition, category, account_type_val, provider_type, description, annualized_date_range, all_time_date_range, all_annualized, all_all_time, has_practice_info_annualized, has_practice_info_all_time, has_provider_info_annualized, has_provider_info_all_time)

DROP TABLE IF EXISTS ai_adm_asset_inventory_hcv;

CREATE temp TABLE ai_adm_asset_inventory_hcv AS
SELECT 'Analyte Analysis' category,
       account_type_val,
       provider_type,
       'Total Number of ' ||test_result_name_stnd|| ' results' description,
       SUM(CASE WHEN is_last_year THEN record_ct ELSE 0 END) all_annualized,
       SUM(record_ct) all_all_time,
       SUM(CASE WHEN is_last_year AND has_practice_info = 1 THEN record_ct ELSE 0 END) has_practice_info_annualized,
       SUM(CASE WHEN has_practice_info THEN record_ct ELSE 0 END) has_practice_info_all_time,
       SUM(CASE WHEN is_last_year AND has_provider_info = 1 THEN record_ct ELSE 0 END) has_provider_info_annualized,
       SUM(CASE WHEN has_provider_info THEN record_ct ELSE 0 END) has_provider_info_all_time,
       SUM(CASE WHEN is_last_year AND has_npi = 1 THEN record_ct ELSE 0 END) has_npi_annualized,
       SUM(CASE WHEN has_npi THEN record_ct ELSE 0 END) has_npi_all_time
FROM (SELECT account_type_val,
             provider_type,
             test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)AND test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)-100 is_last_year,
             has_practice_info,
             has_provider_info,
             has_npi,
             test_result_name_stnd,
             COUNT(DISTINCT patient_date_id) record_ct
      FROM ai_ppdm_hcv
      GROUP BY account_type_val,
               provider_type,
               test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)AND test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hcv)-100,
               has_practice_info,
               has_provider_info,
               has_npi,
               test_result_name_stnd) t
GROUP BY account_type_val,
         provider_type,
         test_result_name_stnd
UNION
SELECT 'Practice Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of practices in the data with tests' description,
       MAX(al.practice_ct) all_annualized,
       MAX(ah.practice_ct) all_all_time,
       MAX(pracl.practice_ct) has_practice_info_annualized,
       MAX(prach.practice_ct) has_practice_info_all_time,
       MAX(provl.practice_ct) has_provider_info_annualized,
       MAX(provh.practice_ct) has_provider_info_all_time,
       MAX(npil.practice_ct) has_npi_annualized,
       MAX(npih.practice_ct) has_npi_all_time
       
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type         
    
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Practice Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of practices in the data with at least one Dx patient' description,
       MAX(al.practice_ct_with_diag) all_annualized,
       MAX(ah.practice_ct_with_diag) all_all_time,
       MAX(pracl.practice_ct_with_diag) has_practice_info_annualized,
       MAX(prach.practice_ct_with_diag) has_practice_info_all_time,
       MAX(provl.practice_ct_with_diag) has_provider_info_annualized,
       MAX(provh.practice_ct_with_diag) has_provider_info_all_time,
        MAX(npil.practice_ct_with_diag) has_npi_annualized,
       MAX(npih.practice_ct_with_diag) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           

GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Provider Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of providers in the data with tests' description,
       MAX(al.provider_ct) all_annualized,
       MAX(ah.provider_ct) all_all_time,
       MAX(pracl.provider_ct) has_practice_info_annualized,
       MAX(prach.provider_ct) has_practice_info_all_time,
       MAX(provl.provider_ct) has_provider_info_annualized,
       MAX(provh.provider_ct) has_provider_info_all_time,
      MAX(npil.provider_ct) has_npi_annualized,
       MAX(npih.provider_ct) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           



GROUP BY ah.account_type_val,
         ah.provider_type
         

UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of providers in the data with at least one Dx patient' description,
       MAX(al.provider_ct_with_diag) all_annualized,
       MAX(ah.provider_ct_with_diag) all_all_time,
       MAX(pracl.provider_ct_with_diag) has_practice_info_annualized,
       MAX(prach.provider_ct_with_diag) has_practice_info_all_time,
       MAX(provl.provider_ct_with_diag) has_provider_info_annualized,
       MAX(provh.provider_ct_with_diag) has_provider_info_all_time,
       MAX(npil.provider_ct_with_diag) has_npi_annualized,
       MAX(npih.provider_ct_with_diag) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type



UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Diagnosed HCV Patients' description,
       MAX(al.patients_with_diag) all_annualized,
       MAX(ah.patients_with_diag) all_all_time,
       MAX(pracl.patients_with_diag) has_practice_info_annualized,
       MAX(prach.patients_with_diag) has_practice_info_all_time,
       MAX(provl.patients_with_diag) has_provider_info_annualized,
       MAX(provh.patients_with_diag) has_provider_info_all_time,
       MAX(npil.patients_with_diag) has_npi_annualized,
       MAX(npih.patients_with_diag) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type  
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients Pending/Lost' description,
       MAX(al.patients_is_in_treatment_pending_lost) all_annualized,
       MAX(ah.patients_is_in_treatment_pending_lost) all_all_time,
       MAX(pracl.patients_is_in_treatment_pending_lost) has_practice_info_annualized,
       MAX(prach.patients_is_in_treatment_pending_lost) has_practice_info_all_time,
       MAX(provl.patients_is_in_treatment_pending_lost) has_provider_info_annualized,
       MAX(provh.patients_is_in_treatment_pending_lost) has_provider_info_all_time,
       MAX(npil.patients_is_in_treatment_pending_lost) has_npi_annualized,
       MAX(npih.patients_is_in_treatment_pending_lost) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients under treatment, success' description,
       MAX(al.patients_is_in_treatment_success) all_annualized,
       MAX(ah.patients_is_in_treatment_success) all_all_time,
       MAX(pracl.patients_is_in_treatment_success) has_practice_info_annualized,
       MAX(prach.patients_is_in_treatment_success) has_practice_info_all_time,
       MAX(provl.patients_is_in_treatment_success) has_provider_info_annualized,
       MAX(provh.patients_is_in_treatment_success) has_provider_info_all_time,
       MAX(npil.patients_is_in_treatment_success) has_npi_annualized,
       MAX(npih.patients_is_in_treatment_success) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients under treatment, not at goal' description,
       MAX(al.patients_is_in_treatment_not_at_goal) all_annualized,
       MAX(ah.patients_is_in_treatment_not_at_goal) all_all_time,
       MAX(pracl.patients_is_in_treatment_not_at_goal) has_practice_info_annualized,
       MAX(prach.patients_is_in_treatment_not_at_goal) has_practice_info_all_time,
       MAX(provl.patients_is_in_treatment_not_at_goal) has_provider_info_annualized,
       MAX(provh.patients_is_in_treatment_not_at_goal) has_provider_info_all_time,
      MAX(npil.patients_is_in_treatment_not_at_goal) has_npi_annualized,
       MAX(npih.patients_is_in_treatment_not_at_goal) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type

UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients warehoused' description,
       MAX(al.patients_is_warehoused) all_annualized,
       MAX(ah.patients_is_warehoused) all_all_time,
       MAX(pracl.patients_is_warehoused) has_practice_info_annualized,
       MAX(prach.patients_is_warehoused) has_practice_info_all_time,
       MAX(provl.patients_is_warehoused) has_provider_info_annualized,
       MAX(provh.patients_is_warehoused) has_provider_info_all_time,
       MAX(npil.patients_is_warehoused) has_npi_annualized,
       MAX(npih.patients_is_warehoused) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients tested for HCV Genotype test' description,
       MAX(al.patients_with_gt_test) all_annualized,
       MAX(ah.patients_with_gt_test) all_all_time,
       MAX(pracl.patients_with_gt_test) has_practice_info_annualized,
       MAX(prach.patients_with_gt_test) has_practice_info_all_time,
       MAX(provl.patients_with_gt_test) has_provider_info_annualized,
       MAX(provh.patients_with_gt_test) has_provider_info_all_time,
      MAX(npil.patients_with_gt_test) has_npi_annualized,
       MAX(npih.patients_with_gt_test) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients tested for HCV Genotype 1' description,
       MAX(al.patients_with_gt1_test) all_annualized,
       MAX(ah.patients_with_gt1_test) all_all_time,
       MAX(pracl.patients_with_gt1_test) has_practice_info_annualized,
       MAX(prach.patients_with_gt1_test) has_practice_info_all_time,
       MAX(provl.patients_with_gt1_test) has_provider_info_annualized,
       MAX(provh.patients_with_gt1_test) has_provider_info_all_time,
       MAX(npil.patients_with_gt1_test) has_npi_annualized,
       MAX(npih.patients_with_gt1_test) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients tested for HCV Genotype 1a' description,
       MAX(al.patients_with_gt1a_test) all_annualized,
       MAX(ah.patients_with_gt1a_test) all_all_time,
       MAX(pracl.patients_with_gt1a_test) has_practice_info_annualized,
       MAX(prach.patients_with_gt1a_test) has_practice_info_all_time,
       MAX(provl.patients_with_gt1a_test) has_provider_info_annualized,
       MAX(provh.patients_with_gt1a_test) has_provider_info_all_time,
     MAX(npil.patients_with_gt1a_test) has_npi_annualized,
       MAX(npih.patients_with_gt1a_test) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
UNION
SELECT 'Patient Profiles' category,
       ah.account_type_val,
       ah.provider_type,
       'Patients tested for HCV Genotype 1b' description,
       MAX(al.patients_with_gt1b_test) all_annualized,
       MAX(ah.patients_with_gt1b_test) all_all_time,
       MAX(pracl.patients_with_gt1b_test) has_practice_info_annualized,
       MAX(prach.patients_with_gt1b_test) has_practice_info_all_time,
       MAX(provl.patients_with_gt1b_test) has_provider_info_annualized,
       MAX(provh.patients_with_gt1b_test) has_provider_info_all_time,
       MAX(npil.patients_with_gt1b_test) has_npi_annualized,
       MAX(npih.patients_with_gt1b_test) has_npi_all_time
FROM practice_provider_all_hist ah
  INNER JOIN practice_provider_all_ly al
          ON al.account_type_val = ah.account_type_val
         AND al.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_hist prach
          ON prach.account_type_val = ah.account_type_val
         AND prach.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prac_ly pracl
          ON pracl.account_type_val = ah.account_type_val
         AND pracl.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_hist provh
          ON provh.account_type_val = ah.account_type_val
         AND provh.provider_type = ah.provider_type
  INNER JOIN practice_provider_map2prov_ly provl
          ON provl.account_type_val = ah.account_type_val
         AND provl.provider_type = ah.provider_type
 INNER JOIN practice_provider_map2NPI_hist npih
          ON npih.account_type_val=ah.account_type_val
	 				AND npih.provider_type=ah.provider_type
  INNER JOIN practice_provider_map2NPI_ly npil
          ON npil.account_type_val=ah.account_type_val
					AND npil.provider_type=ah.provider_type           
GROUP BY ah.account_type_val,
         ah.provider_type
ORDER BY account_type_val,
         category,
         description;

-- Section 6: Populate date ranges and set condition name field

ALTER TABLE ai_adm_asset_inventory_hcv ADD annualized_date_range VARCHAR(30);
ALTER TABLE ai_adm_asset_inventory_hcv ADD all_time_date_range VARCHAR(30);

UPDATE ai_adm_asset_inventory_hcv
   SET annualized_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_ly)
   ,all_time_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_hist);

ALTER TABLE ai_adm_asset_inventory_hcv ADD condition VARCHAR(15);

UPDATE ai_adm_asset_inventory_hcv
   SET condition = 'HCV';





-- Section 7: Merge with main table

DELETE
FROM ai_adm_asset_inventory
WHERE condition = 'HCV';

insert into ai_adm_asset_inventory
SELECT condition,
       category,
       account_type_val,
       provider_type,
       description,
       annualized_date_range,
       all_time_date_range,
       all_annualized,
       all_all_time,
       has_practice_info_annualized,
       has_practice_info_all_time,
       has_provider_info_annualized,
       has_provider_info_all_time,
       has_npi_annualized,
       has_npi_all_time
FROM ai_adm_asset_inventory_hcv;

/*
SELECT *
FROM ai_adm_asset_inventory
ORDER by condition, category, description;
*/

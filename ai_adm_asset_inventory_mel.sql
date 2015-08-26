
DROP TABLE IF EXISTS practice_provider_all_hist;

CREATE TEMP TABLE practice_provider_all_hist as

SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(patient_date_id) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id ) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation 
FROM ai_ppdm_mel
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)  and braf_result_mel = 1
GROUP BY 
1,2 ;

--select distinct practice_id from ai_ppdm_mel where braf_result_mel = 1


DROP TABLE IF EXISTS practice_provider_all_ly;

CREATE TEMP TABLE practice_provider_all_ly as
SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation 

FROM ai_ppdm_mel
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel ) -100  and braf_result_mel = 1
GROUP BY 
1,2;


-- count of all the provider mapped 2 practice
DROP TABLE IF EXISTS practice_provider_map2prac_hist;

CREATE TEMP TABLE practice_provider_map2prac_hist as
SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation   

FROM ai_ppdm_mel
WHERE has_practice_info = 'true'
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) and  braf_result_mel = 1
GROUP BY 1,2;




DROP TABLE IF EXISTS practice_provider_map2prac_ly;

CREATE TEMP TABLE practice_provider_map2prac_ly as
 
SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation   

FROM ai_ppdm_mel
WHERE has_practice_info = 'true'
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) -100  and braf_result_mel = 1
GROUP BY 
1,2;


-- Calculate all mapped 2 provider
DROP TABLE IF EXISTS practice_provider_map2prov_hist;

CREATE TEMP TABLE practice_provider_map2prov_hist as
SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation   
FROM ai_ppdm_mel 
WHERE has_provider_info = 'true'
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) -100  and braf_result_mel = 1
GROUP BY 1,2;


DROP TABLE IF EXISTS practice_provider_map2prov_ly;
CREATE TEMP TABLE practice_provider_map2prov_ly as
SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation  

FROM ai_ppdm_mel
WHERE has_provider_info = 'true'
AND   test_observation_reported_date_yyyymm < (SELECT 
MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) -100  and braf_result_mel = 1
GROUP BY 1,2;





DROP TABLE IF EXISTS practice_provider_map2NPI_hist;

SELECT 
	   account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation   
	   into temp table practice_provider_map2NPI_hist
FROM ai_ppdm_mel
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) and  braf_result_mel = 1
GROUP BY 
       account_type_val,
        provider_type;
	 
	

DROP TABLE IF EXISTS practice_provider_map2NPI_ly;

SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
 	     COUNT(CASE WHEN braf_result_mel = 1 THEN patient_date_id END) test_count,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT CASE WHEN braf_result_mel = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN new_pats = 1 THEN patient_id END) new_pats,
       COUNT(DISTINCT CASE WHEN has_braf_pstv_mutation  = 1 THEN patient_id END)  has_braf_pstv_mutation  
	   into temp table practice_provider_map2NPI_ly
FROM ai_ppdm_mel
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel) -100 and braf_result_mel = 1
GROUP BY 
        account_type_val,
         provider_type;



DROP TABLE IF EXISTS ai_adm_asset_inventory_mel;

CREATE TABLE ai_adm_asset_inventory_mel AS
SELECT 'Analyte Analysis' category,
       account_type_val,
       provider_type,
       'Total Number of ' ||test_result_name_stnd || ' results' description,
       SUM(CASE WHEN is_last_year THEN record_ct ELSE 0 END) all_annualized,
       SUM(record_ct) all_all_time,
       SUM(CASE WHEN is_last_year AND has_practice_info = 1 THEN record_ct ELSE 0 END) has_practice_info_annualized,
       SUM(CASE WHEN has_practice_info THEN record_ct ELSE 0 END) has_practice_info_all_time,
       SUM(CASE WHEN is_last_year AND has_provider_info = 1 THEN record_ct ELSE 0 END) has_provider_info_annualized,
       SUM(CASE WHEN has_provider_info THEN record_ct ELSE 0 END) has_provider_info_all_time,
       SUM(CASE WHEN is_last_year AND has_npi = 1 THEN record_ct ELSE 0 END) has_npi_annualized,
       SUM(CASE WHEN has_npi THEN record_ct ELSE 0 END) has_npi_all_time
FROM (SELECT 
             account_type_val,
             provider_type,
             test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)AND test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)-100 is_last_year,
             has_practice_info,
             has_provider_info,
             has_npi,
             test_result_name_stnd,
             COUNT(DISTINCT patient_date_id) record_ct
      FROM ai_ppdm_mel
      GROUP BY 
               account_type_val,               
    	 provider_type,
               test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)AND test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_mel)-100,
               has_practice_info,
               has_provider_info,
               has_npi,
               test_result_name_stnd) 
GROUP BY account_type_val,
         provider_type,
         test_result_name_stnd
UNION
SELECT 'Analyte Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of Melanoma BRAF tests' description,
       MAX(al.test_count) all_annualized,
       MAX(ah.test_count) all_all_time,
       MAX(pracl.test_count) has_practice_info_annualized,
       MAX(prach.test_count) has_practice_info_all_time,
       MAX(provl.test_count) has_provider_info_annualized,
       MAX(provh.test_count) has_provider_info_all_time,
       MAX(npil.test_count) has_npi_annualized,
       MAX(npih.test_count) has_npi_all_time
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
       'Total number of Practices in the data with Tests' description,
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
SELECT 'Provider Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of providers in the data with atleast 1 Dx patient' description,
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
       'Newly Identified Patients' description,
       MAX(al.new_pats) all_annualized,
       MAX(ah.new_pats) all_all_time,
       MAX(pracl.new_pats) has_practice_info_annualized,
       MAX(prach.new_pats) has_practice_info_all_time,
       MAX(provl.new_pats) has_provider_info_annualized,
       MAX(provh.new_pats) has_provider_info_all_time,
       MAX(npil.new_pats) has_npi_annualized,
       MAX(npih.new_pats) has_npi_all_time
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
       'Patients with Positive BRAF Mutation' description,
       MAX(al.has_braf_pstv_mutation) all_annualized,
       MAX(ah.has_braf_pstv_mutation) all_all_time,
       MAX(pracl.has_braf_pstv_mutation) has_practice_info_annualized,
       MAX(prach.has_braf_pstv_mutation) has_practice_info_all_time,
       MAX(provl.has_braf_pstv_mutation) has_provider_info_annualized,
       MAX(provh.has_braf_pstv_mutation) has_provider_info_all_time,
       MAX(npil.has_braf_pstv_mutation) has_npi_annualized,
       MAX(npih.has_braf_pstv_mutation) has_npi_all_time
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
         ah.provider_type;
         





-- SET DATE RANGES
ALTER TABLE ai_adm_asset_inventory_mel ADD annualized_date_range VARCHAR(30);
ALTER TABLE ai_adm_asset_inventory_mel ADD all_time_date_range VARCHAR(30);

UPDATE ai_adm_asset_inventory_mel
   SET annualized_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_ly)
   ,all_time_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_hist);

-- SET CONDITION NAME
ALTER TABLE ai_adm_asset_inventory_mel ADD condition VARCHAR(15);

UPDATE ai_adm_asset_inventory_mel
   SET condition = 'BRAF';

DELETE 
FROM  ai_adm_asset_inventory
WHERE condition = 'BRAF';

INSERT INTO  ai_adm_asset_inventory
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
FROM ai_adm_asset_inventory_mel;






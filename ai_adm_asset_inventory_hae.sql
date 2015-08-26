


SET search_path to analytics_dev,'$user',public;



DROP TABLE IF EXISTS practice_provider_all_hist;

SELECT 
       account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_all_hist
FROM ai_ppdm_hae
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
GROUP BY
        account_type_val,
        provider_type;

		 
DROP TABLE IF EXISTS practice_provider_all_ly;

SELECT 
      account_type_val,
       provider_type,
       MAX(test_observation_reported_date_dt) last_test_date,
       MIN(test_observation_reported_date_dt) first_test_date,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
---Provider count with atleast 1 dx patient
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_all_ly
FROM ai_ppdm_hae
WHERE test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae) -100
GROUP BY 
	account_type_val,
        provider_type;

		 
DROP TABLE IF EXISTS practice_provider_map2prac_hist;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_map2prac_hist
FROM ai_ppdm_hae
WHERE has_practice_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
GROUP BY 
account_type_val,
        provider_type;

		 
DROP TABLE IF EXISTS practice_provider_map2prac_ly;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_map2prac_ly
FROM ai_ppdm_hae
WHERE has_practice_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae) -100
GROUP BY 
	 account_type_val,
         provider_type;

		 

DROP TABLE IF EXISTS practice_provider_map2prov_hist;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients 
	   INTO temp TABLE practice_provider_map2prov_hist
FROM ai_ppdm_hae
WHERE has_provider_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
GROUP BY 
	 account_type_val,
         provider_type;
         

DROP TABLE IF EXISTS practice_provider_map2prov_ly;
/* Data within one year*/
SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients 
	  INTO temp TABLE practice_provider_map2prov_ly
FROM ai_ppdm_hae
WHERE has_provider_info = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae) -100
GROUP BY 
        account_type_val,
         provider_type;
	

---NPI INFORMATION

DROP TABLE IF EXISTS practice_provider_map2NPI_hist;

SELECT 
	   account_type_val,
       provider_type,
       COUNT(DISTINCT patient_date_id) record_ct,
       COUNT(DISTINCT practice_id) AS practice_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN practice_id END) practice_ct_with_diag,
       COUNT(DISTINCT provider_id) AS provider_ct,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN provider_id END) provider_ct_with_diag,
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_map2NPI_hist
FROM ai_ppdm_hae
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
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
       COUNT(DISTINCT CASE WHEN has_diag = 1 THEN patient_id END) patients_dx,
       COUNT(DISTINCT CASE WHEN Consistent_to_HAE_patients = 1 THEN patient_id END) Consistent_to_HAE_patients,
	   COUNT(DISTINCT CASE WHEN Equivocal_for_HAE_patients = 1 THEN patient_id END) Equivocal_for_HAE_patients
	   into temp table practice_provider_map2NPI_ly
FROM ai_ppdm_hae
WHERE has_npi = 1
AND   test_observation_reported_date_yyyymm < (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)
AND   test_observation_reported_date_yyyymm >= (SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae) -100
GROUP BY 
        account_type_val,
         provider_type;




	 
DROP TABLE IF EXISTS ai_adm_asset_inventory_hae;

CREATE TEMP TABLE ai_adm_asset_inventory_hae AS
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
       SUM(CASE WHEN has_npi =1 THEN record_ct ELSE 0 END) has_npi_all_time

FROM (SELECT account_type_val,
             provider_type,
             test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)AND 
			 test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)-100 is_last_year,
             has_practice_info,
             has_provider_info,
             has_npi,
             test_result_name_stnd,
             COUNT(DISTINCT patient_date_id) record_ct
      FROM ai_ppdm_hae
      GROUP BY 
   	    account_type_val,
               provider_type,
               test_observation_reported_date_yyyymm <(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)AND test_observation_reported_date_yyyymm >=(SELECT MAX(test_observation_reported_date_yyyymm) FROM ai_ppdm_hae)-100,
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

SELECT 'Provider Analysis' category,
       ah.account_type_val,
       ah.provider_type,
       'Total number of providers in the data with at least 1 Dx patient' description,
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
       'Patients diagnosed with HAE' description,
       MAX(al.patients_dx) all_annualized,
       MAX(ah.patients_dx) all_all_time,
       MAX(pracl.patients_dx) has_practice_info_annualized,
       MAX(prach.patients_dx) has_practice_info_all_time,
       MAX(provl.patients_dx) has_provider_info_annualized,
       MAX(provh.patients_dx) has_provider_info_all_time,
MAX(npil.patients_dx) has_npi_annualized,
       MAX(npih.patients_dx) has_npi_all_time

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
       'Patients consitent to HAE' description,
       MAX(al.Consistent_to_HAE_patients) all_annualized,
       MAX(ah.Consistent_to_HAE_patients) all_all_time,
       MAX(pracl.Consistent_to_HAE_patients) has_practice_info_annualized,
       MAX(prach.Consistent_to_HAE_patients) has_practice_info_all_time,
       MAX(provl.Consistent_to_HAE_patients) has_provider_info_annualized,
       MAX(provh.Consistent_to_HAE_patients) has_provider_info_all_time,
MAX(npil.Consistent_to_HAE_patients) has_npi_annualized,
       MAX(npih.Consistent_to_HAE_patients) has_npi_all_time

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
       'Patients Equivocal to HAE' description,
       MAX(al.Equivocal_for_HAE_patients) all_annualized,
       MAX(ah.Equivocal_for_HAE_patients) all_all_time,
       MAX(pracl.Equivocal_for_HAE_patients) has_practice_info_annualized,
       MAX(prach.Equivocal_for_HAE_patients) has_practice_info_all_time,
       MAX(provl.Equivocal_for_HAE_patients) has_provider_info_annualized,
       MAX(provh.Equivocal_for_HAE_patients) has_provider_info_all_time,
MAX(npil.Equivocal_for_HAE_patients) has_npi_annualized,
       MAX(npih.Equivocal_for_HAE_patients) has_npi_all_time

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
  

    



ALTER TABLE ai_adm_asset_inventory_hae ADD annualized_date_range VARCHAR(30);
ALTER TABLE ai_adm_asset_inventory_hae ADD all_time_date_range VARCHAR(30);

UPDATE ai_adm_asset_inventory_hae
   SET annualized_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_ly)
   ,all_time_date_range = (select min(first_test_date)||' - '|| max(last_test_date) last_test_date_all from practice_provider_all_hist);

-- SET CONDITION NAME
ALTER TABLE ai_adm_asset_inventory_hae ADD condition VARCHAR(15);

UPDATE ai_adm_asset_inventory_hae
   SET condition = 'HAE';

DELETE
FROM ai_adm_asset_inventory
WHERE condition = 'HAE';



INSERT INTO ai_adm_asset_inventory
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
FROM ai_adm_asset_inventory_hae;


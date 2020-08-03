-- This table selects:
  -- combine static compliance and pf ratio (the closet within the same 4hour time window)
-- 1870 patients


drop table if exists `ync-capstones.Jia.combined_firstpf_compliance`;
create table `ync-capstones.Jia.combined_firstpf_compliance`  as

with v1 as (
select p.*,l.max_peep, l.peep,l.tidal_volume,l.plateau_pressure,l.TV_offset as lung_offset, l.lung_compliance,TV_IBW
,l.first_tv/p.IBW_calculated as TV_IBW_calculated -- use initial tidal volume
, ROW_NUMBER() OVER (partition by p.patientunitstayid order by ABS(fio2_offset-l.TV_offset) asc) as ranked_by_minute_diff
from `ync-capstones.Jia.patient_first_pfratio`  p
inner join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and lung_time=fio2_time
order by p.patientunitstayid, fio2_offset),

first as ( -- combine pf ratio with lung compliance 
select *
from v1
where ranked_by_minute_diff = 1
order by patientunitstayid,fio2_offset),

icd_code AS (
SELECT
diag.patientunitstayid,
SAFE_CAST(SUBSTR(diag.icd9code, 0, 3) as INT64) AS icd9code,
icd9code AS icd9code_string
FROM `physionet-data.eicu_crd.diagnosis` diag),

icd_presence AS (
SELECT
icd_code.patientunitstayid,
COUNT(CASE WHEN icd_code.icd9code BETWEEN 427 AND 427 THEN 1 END) > 0 AS has_atrial_fibrillation_disease,
COUNT(CASE WHEN icd_code.icd9code_string LIKE '%427.31%' THEN 1 END) > 0 AS AF, -- Atrial fibrillation
COUNT(CASE WHEN icd_code.icd9code BETWEEN 140 AND 209 THEN 1 END) > 0 AS has_cancer_disease, -- Cancer T/F
COUNT(CASE WHEN icd_code.icd9code_string LIKE '%428%' THEN 1 END) > 0 AS CHF, -- Congestive heart failure
COUNT(CASE WHEN icd_code.icd9code_string LIKE '%585%' THEN 1 END) > 0 AS CKD, -- Chronic kidney disease
COUNT(CASE WHEN icd_code.icd9code BETWEEN 571 AND 571 THEN 1 END) > 0 AS has_chronic_liver_disease,
COUNT(CASE WHEN icd_code.icd9code BETWEEN 490 AND 496 THEN 1 END) > 0 AS has_copd_disease, --chronic_obstructive_pulmonary_disease
COUNT(CASE WHEN icd_code.icd9code BETWEEN 250 AND 250 THEN 1 END) > 0 AS has_diabetes_mellitus_disease,
COUNT(CASE WHEN icd_code.icd9code BETWEEN 401 AND 405 THEN 1 END) > 0 AS has_hypertension_disease,
COUNT(CASE WHEN icd_code.icd9code BETWEEN 410 AND 414 THEN 1 END) > 0 AS has_ischemic_heart_disease,
COUNT(CASE WHEN icd_code.icd9code BETWEEN 038 AND 038 THEN 1 END) > 0 AS has_sepsis, 
COUNT(CASE WHEN icd_code.icd9code BETWEEN 434 AND 434 THEN 1 END) > 0 AS has_stroke_disease,
FROM icd_code
GROUP BY icd_code.patientunitstayid),

Temperature as ( 
select
patientunitstayid,temperature
from ( select patientunitstayid,temperature
, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY observationoffset asc) as temp_rank
from `physionet-data.eicu_crd.vitalperiodic`)
where temperature is not null
and temp_rank=1),

Heartrate as ( 
select
patientunitstayid,heartrate 
from ( select patientunitstayid,heartrate
, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY observationoffset asc) as heart_rank
from `physionet-data.eicu_crd.vitalperiodic`)
where heartrate is not null
and heart_rank=1),

Respiration as ( 
select
patientunitstayid,respiration
from ( select patientunitstayid,respiration
, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY observationoffset asc) as resp_rank
from `physionet-data.eicu_crd.vitalperiodic`)
where respiration is not null
and resp_rank=1),

mean_arterial_pressure as ( 
select
patientunitstayid,mean_arterial_pressure
from ( select patientunitstayid
, systemicsystolic*1/3+systemicdiastolic*2/3 as mean_arterial_pressure 
, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY observationoffset asc) as map_rank
from `physionet-data.eicu_crd.vitalperiodic`)
where mean_arterial_pressure is not null
and map_rank=1
),

v2 as (
select 
first.*, sofa.sofatotal , Temperature.temperature, Heartrate.heartrate, Respiration.respiration, mean_arterial_pressure.mean_arterial_pressure
, icd_presence.* EXCEPT(patientunitstayid)
from first
left outer join Temperature
on first.patientunitstayid= Temperature.patientunitstayid
left outer join Heartrate
on first.patientunitstayid= Heartrate.patientunitstayid
left outer join Respiration
on first.patientunitstayid= Respiration.patientunitstayid
left outer join mean_arterial_pressure
on first.patientunitstayid= mean_arterial_pressure.patientunitstayid
left join icd_presence
on first.patientunitstayid= icd_presence.patientunitstayid
left join `ync-capstones.Jia.sofa` sofa
on first.patientunitstayid= sofa.patientunitstayid)

select v2.* from v2




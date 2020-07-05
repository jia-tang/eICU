-- This table selects:
-- invasively ventilated on the first day of ICU admission
-- Patients who are invasively ventilated for at least 48 hours
-- PF ratio < 300
    -- PF ratio refers to the first recorded PF ratio within the first day of ICU admission. (use first recorded fio2_offset, take the first recorded after restricting pfratio <300)
    -- Take pf ratio with fio2_offset closest to pao2_offset, within 4 hour apart
    -- labeld with icd9 code
    

drop view if exists `ync-capstones.Jia.patient_first_pfratio`;
create view `ync-capstones.Jia.patient_first_pfratio` as
with allICU as -- patient table
  (select 
    --uniquepid, 
    --patienthealthsystemstayid, 
    patientunitstayid,	
    gender,
    safe_cast(age as numeric) as age,
    ethnicity,
    --hospitaladmitoffset,
    unitdischargeoffset as unit_duration,
    hospitaldischargeoffset - hospitaladmitoffset as hospital_duration,
    unitAdmitSource,
    unitdischargestatus,
    hospitaldischargestatus,
    admissionheight,
    admissionweight,
    dischargeweight,
    ROW_NUMBER() OVER (PARTITION BY patienthealthsystemstayid ORDER BY hospitaladmitoffset) as ICU_rank
  --   hospitalid,
  --   wardid,
  --   apacheadmissiondx,
  --   hospitaladmitsource,
  --   hospitaldischargeyear,
  --   hospitaldischargetime24, 
  --   hospitaladmittime24,
  --   hospitaldischargelocation,
  --   unittype,
  --   unitadmitsource,
  --   unitvisitnumber,
  --   unitstaytype,
  --   admissionweight,
  --   dischargeweight,
  --   unitdischargetime24,
  --   unitdischargelocation		
  from `physionet-data.eicu_crd.patient` -- table of ICU stays confusingly called "patients"
  ),
  
int as (
select distinct patientunitstayid from 
`physionet-data.eicu_crd.apachepredvar`
where oobintubday1=1), --52933 invasively ventilated on first day of admission
  
on_mech_vent as(
  select icu.*,vent_start,vent_duration
    ,case
        WHEN gender = "Female" THEN 50+(0.91*admissionheight-152.4)
        WHEN gender ="Male" THEN 45.5+(0.91*admissionheight-152.4)
    END AS IBW_calculated
  ,  COALESCE(NULL, NULL, admissionweight,dischargeweight)/nullif(power(admissionheight/100,2),0) as bmi -- use admissionweight. If null then use dischargeweight
  from allICU icu 
  inner join int
  on int.patientunitstayid=icu.patientunitstayid
  inner join `ync-capstones.Jia.oxygen_therapy` ox
  on icu.patientunitstayid = ox.icustay_id
  where 
   vent_duration > 48 -- 29058 ventilated >48h
  ),
  
------------------------------------------------------------------------------------------------------------------------
-- now we compute PF ratios

pao2 as --pao2 from lab
(
select lab.patientunitstayid, labresult as pao2, lab.labresultoffset
from 
  (select * 
  from `physionet-data.eicu_crd.lab` lab
  where lower(labname) like 'pao2%') lab
left outer join on_mech_vent mv 
on lab.patientunitstayid = mv.patientunitstayid
where labresultoffset between vent_start and 60*24 + vent_start -- first 1 days of mech ventilation 
-- group by patientunitstayid
),
    
fio2 as (
SELECT
       distinct rp.patientunitstayid, respchartoffset,
      case 
              when respchartvaluelabel="Set Fraction of Inspired Oxygen (FIO2)"
                then SAFE_CAST(respchartvalue AS numeric) 
              when respchartvaluelabel="FiO2"
                then SAFE_CAST(respchartvalue AS numeric)
              when respchartvaluelabel="FIO2 (%)"
                then SAFE_CAST(respchartvalue AS numeric)*100 
                end
       as fio2,
    FROM
      `physionet-data.eicu_crd.respiratorycharting` rp
      left outer join on_mech_vent mv 
      on rp.patientunitstayid = mv.patientunitstayid
      WHERE
      respchartoffset between vent_start and 60*24 + vent_start
      and SAFE_CAST(respchartvalue AS numeric) >0
      and respchartvalue is not null
      order by patientunitstayid,respchartoffset),
    
pf_ratio as 
(select mv.*,fio2.fio2, 100 * pao2.pao2 / fio2.fio2 as pfratio, fio2.respchartoffset as fio2_offset, pao2.labresultoffset as pao2_offset
from fio2
inner join pao2 
on fio2.patientunitstayid = pao2.patientunitstayid
inner join on_mech_vent mv
on mv.patientunitstayid = fio2.patientunitstayid
where fio2.respchartoffset between pao2.labresultoffset - 4*60 and pao2.labresultoffset + 4*60
-- values are less than 4 hour apart
),

final as (select *
, ROW_NUMBER() OVER (partition by pf_ratio.patientunitstayid order by ABS(fio2_offset-pao2_offset) asc) as ranked_pf_diff
from pf_ratio
where pfratio is not null),

first_pf as 
(SELECT * except(ranked_pf_diff, fio2_rank),
    CASE 
        WHEN pfratio <= 100 THEN "severe"
        WHEN pfratio between 100 and 200 THEN "moderate"
        WHEN pfratio between 200 and 300 THEN "mild"
    END AS ARDS_severity
FROM (select *
,ROW_NUMBER() OVER (PARTITION BY final.patientunitstayid ORDER BY fio2_offset) as fio2_rank 
from final 
where ranked_pf_diff=1)
where fio2_rank =1
and pfratio<=300
order by patientunitstayid),

label as (
select distinct patientunitstayid
from `physionet-data.eicu_crd.diagnosis`
where diagnosisstring like  "%ARDS%" or icd9code like "%518.82%" or icd9code like "%518.5%" or icd9code like "%518.81%" 
)

select first_pf.* from first_pf
inner join label
on label.patientunitstayid=first_pf.patientunitstayid

--6632 patients

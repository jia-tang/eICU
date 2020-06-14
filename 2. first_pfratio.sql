-- This table selects:
-- mechanical ventilated
-- Patients who are invasively ventilated for at least 48 hours
-- PF ratio < 300
    -- PF ratio refers to the first recorded PF ratio within the first day of ICU admission. (use first recorded fio2_offset, take the first recorded after restricting pfratio <300)
    -- Take pf ratio with fio2_offset closest to pao2_offset, within 1 hour apart
    

drop view if exists `ync-capstones.Jia.patient_min_pfratio`;
create view `ync-capstones.Jia.patient_min_pfratio` as
with allICU as -- patient table
  (select 
    --uniquepid, 
    --patienthealthsystemstayid, 
    patientunitstayid,	
    gender,
    age,
    ethnicity,
    --hospitaladmitoffset,
    unitdischargeoffset,
    --hospitaldischargeoffset,
    unitdischargestatus,
    --hospitaldischargestatus,
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
  -- where unitDischargeOffset > 12*60 -- unit admission longer than 12 hours
  ),
  
on_mech_vent as(
  select icu.patientunitstayid,	unitdischargeoffset, unitdischargestatus, gender, age, ethnicity,vent_start
    ,case
        WHEN gender = "Female" THEN 50+(0.91*admissionheight-152.4)
        WHEN gender ="Male" THEN 45.5+(0.91*admissionheight-152.4)
    END AS IBW_calculated
  ,  COALESCE(NULL, NULL, admissionweight,dischargeweight)/nullif(power(admissionheight/100,2),0) as bmi -- use admissionweight. If null then use dischargeweight
  from allICU icu 
  inner join `ync-capstones.Jia.oxygen_therapy` ox
  on icu.patientunitstayid = ox.icustay_id
  where ox.ventnum = 1
  and vent_duration > 48
  and oxygen_therapy_type >= 2 -- select mechanical ventilation (Invasive + Noninvasive ventilation)
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


fio2 as --FIO2 from respchart
  (SELECT
      DISTINCT rp.patientunitstayid,
      case 
              when CAST(respchartvalue AS numeric) > 0 and CAST(respchartvalue AS numeric) <= 1
                then CAST(respchartvalue AS numeric) * 100
              -- improperly input data - looks like O2 flow in litres
              when CAST(respchartvalue AS numeric) > 1 and CAST(respchartvalue AS numeric) < 21
                then null
              when CAST(respchartvalue AS numeric) >= 21 and CAST(respchartvalue AS numeric) <= 100
                then CAST(respchartvalue AS numeric)
              else null end -- unphysiological
       as fio2,
      -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
      rp.respchartoffset
    FROM
      `physionet-data.eicu_crd.respiratorycharting` rp
      left outer join on_mech_vent mv 
      on rp.patientunitstayid = mv.patientunitstayid
    WHERE
      respchartoffset between vent_start and 60*24 + vent_start
      AND respchartvalue <> ''
      AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
  ORDER BY
    patientunitstayid),
    
pf_ratio as 
(select mv.*, 100 * pao2.pao2 / fio2.fio2 as pfratio, fio2.respchartoffset as fio2_offset, pao2.labresultoffset as pao2_offset

from fio2
inner join pao2 
on fio2.patientunitstayid = pao2.patientunitstayid
inner join on_mech_vent mv
on mv.patientunitstayid = fio2.patientunitstayid
where fio2.respchartoffset between pao2.labresultoffset - 1*60 and pao2.labresultoffset + 1*60
-- values are less than 1 hour apart
),

final as (select *
, ROW_NUMBER() OVER (partition by pf_ratio.patientunitstayid order by ABS(fio2_offset-pao2_offset) asc) as ranked_pf_diff
from pf_ratio where pfratio<300)

SELECT patientunitstayid, unitdischargestatus, gender, age, ethnicity,IBW_calculated,bmi,vent_start,pfratio,fio2_offset,
    CASE 
        WHEN pfratio <= 100 THEN "severe"
        WHEN pfratio between 100 and 200 THEN "moderate"
        WHEN pfratio between 200 and 300 THEN "mild"
    END AS groupx
FROM (select *
,ROW_NUMBER() OVER (PARTITION BY final.patientunitstayid ORDER BY fio2_offset) as fio2_rank 
from final 
where ranked_pf_diff=1)
where fio2_rank =1
and pfratio<=300
order by patientunitstayid

--17609

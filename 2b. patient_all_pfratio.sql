drop table if exists `ync-capstones.Jia.patient_all_pfratio`;
create table `ync-capstones.Jia.patient_all_pfratio` as
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
  where unitDischargeOffset > 12*60 -- unit admission longer than 12 hours
  ),
  
on_mech_vent as(
  select icu.patientunitstayid,	unitdischargeoffset, unitdischargestatus, gender, age, ethnicity
    ,case
        WHEN gender = "Female" THEN 50+(0.91*admissionheight-152.4)
        WHEN gender ="Male" THEN 45.5+(0.91*admissionheight-152.4)
    END AS IBW_calculated
  ,  COALESCE(NULL, NULL, admissionweight,dischargeweight)/nullif(power(admissionheight/100,2),0) as bmi -- use admissionweight. If null then use dischargeweight
  from allICU icu 
  inner join `ync-capstones.Jia.oxygen_therapy` ox
  on icu.patientunitstayid = ox.icustay_id
  where ox.ventnum = 1),
  
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
      respchartvalue <> ''
      AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
  ORDER BY
    patientunitstayid),
    
pf_ratio as 
(select fio2.patientunitstayid, 100 * pao2.pao2 / fio2.fio2 as pfratio, fio2.respchartoffset as fio2_offset, pao2.labresultoffset as pao2_offset
from fio2
inner join pao2 
on fio2.patientunitstayid = pao2.patientunitstayid
where fio2.respchartoffset between pao2.labresultoffset - 1*60 and pao2.labresultoffset + 1*60
-- values are less than 1 hour apart
), 

final as 
(select mv.*, (pf.fio2_offset + pf.pao2_offset)/2 as pf_offset,pf.pfratio
,ROW_NUMBER() OVER (PARTITION BY pf.patientunitstayid,cast (pfratio as numeric) ORDER BY fio2_offset) as pf_rank
from on_mech_vent mv
inner join pf_ratio pf
on mv.patientunitstayid = pf.patientunitstayid
)

SELECT *, 
    CASE 
        WHEN pfratio <= 100 THEN "severe"
        WHEN pfratio between 100 and 200 THEN "moderate"
        WHEN pfratio between 200 and 300 THEN "mild"
    END AS groupx
FROM final
where pfratio<=300
and pf_rank =1 
order by patientunitstayid
-- 32091

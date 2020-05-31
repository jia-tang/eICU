-- combine static compliance and pf ratio, in a 6 hour interval
-- lung compliance range 7.4-100 
-- Add mortality, TV_IBW calculated

--drop table if exists `ync-capstones.Jia.combined_allpf_compliance`;
--create table `ync-capstones.Jia.combined_allpf_compliance`  as
with v1 as (
select p.patientunitstayid,p.gender,p.bmi,p.ethnicity,p.age, p.pf_offset, l.chartoffset as lung_offset, p.pfratio, p.groupx as ARDS_severity, l.lung_compliance,TV_IBW, (p.pf_offset+l.chartoffset)/2 as pf_lung_offset
,l.tidal_volume/p.IBW_calculated as TV_IBW_calculated
from `ync-capstones.Jia.patient_all_pfratio`  p
right join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and abs(p.pf_offset - l.chartoffset) < 60*6 -- combine pfratio and lung compliance in a 6 hour interval
where pfratio is not null
and lung_compliance > 7.4 -- eliminate rows with compliance too low (<e^2) and too high (>100)
and lung_compliance < 100
order by p.patientunitstayid, pf_offset),

v2 as ( 
select *
,ABS(pf_lung_offset-0) as difference_0hour
,ABS(pf_lung_offset-24*60) as difference_24hour
from v1),

v3 as (
select *
, ROW_NUMBER() OVER (partition by v2.patientunitstayid, lung_offset order by pf_offset ) as firstRowa
, ROW_NUMBER() OVER (partition by v2.patientunitstayid, cast (pf_offset as numeric) order by lung_offset ) as firstRowb
, ROW_NUMBER() OVER (partition by v2.patientunitstayid order by difference_0hour asc) as ranked_0hour
, ROW_NUMBER() OVER (partition by v2.patientunitstayid order by difference_24hour asc) as ranked_24hour
from v2
)

select v3.patientunitstayid,v3.gender,v3.bmi,v3.ethnicity,v3.age, pf_lung_offset, v3.pfratio, v3.ARDS_severity, v3.lung_compliance, m.actualhospitalmortality as mortality, v3.TV_IBW_calculated,v3.TV_IBW
,case WHEN ranked_0hour = 1 and pf_lung_offset<24*60 THEN 1
      WHEN ranked_24hour =1 and pf_lung_offset<48*60 THEN 2 END AS day
from `physionet-data.eicu_crd.apachepatientresult` m
inner join v3
on v3.patientunitstayid=m.patientunitstayid
where firstRowa=1
--ranked_by_minute_diff = 1 # too few data if I only match the compliance that's closed to pf_offset
group by v3.patientunitstayid, pf_lung_offset, v3.pfratio, v3.ARDS_severity, v3.lung_compliance, mortality,gender,bmi,ethnicity,age,TV_IBW_calculated,TV_IBW,ranked_0hour,ranked_24hour
order by patientunitstayid,pf_lung_offset

-- 7480






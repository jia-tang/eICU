-- combine static compliance and pf ratio, in a 6 hour interval (gonna check with KC if it's okay).
-- lung compliance range 0-300 (to be checked)

-- drop table if exists `ync-capstones.Jia.combined_minpf_compliance`;
-- create table `ync-capstones.Jia.combined_minpf_compliance`  as

with v1 as (
select p.patientunitstayid,p.gender,p.bmi,p.ethnicity,p.age, p.pf_offset, l.chartoffset as lung_offset, p.pfratio, p.groupx as ARDS_severity,l.peep, l.lung_compliance,TV_IBW
,l.tidal_volume/p.IBW_calculated as TV_IBW_calculated
,ABS(pf_offset-l.chartoffset) as abs_difference_minute
from `ync-capstones.Jia.patient_min_pfratio`  p
right join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and abs(p.pf_offset - l.chartoffset) < 60*6 -- combine pfratio and lung compliance in a 6 hour interval
where pfratio is not null
and lung_compliance > 7.4 -- eliminate rows with compliance too low (<e^2) and too high (>100)
and lung_compliance < 100
order by p.patientunitstayid, pf_offset),

v2 as ( 
select *
, ROW_NUMBER() OVER (partition by v1.patientunitstayid, lung_offset order by pf_offset ) as firstRowa
, ROW_NUMBER() OVER (partition by v1.patientunitstayid, cast (pf_offset as numeric) order by lung_offset ) as firstRowb
, ROW_NUMBER() OVER (partition by v1.patientunitstayid order by abs_difference_minute asc) as ranked_by_minute_diff
from v1)


select v2.patientunitstayid,v2.gender,v2.bmi,v2.ethnicity,v2.age, (v2.pf_offset+v2.lung_offset)/2 as pf_lung_offset, v2.pfratio, v2.ARDS_severity,v2.peep, v2.lung_compliance, m.actualhospitalmortality as mortality, v2.TV_IBW_calculated,v2.TV_IBW
from `physionet-data.eicu_crd.apachepatientresult` m
inner join v2
on v2.patientunitstayid=m.patientunitstayid
where ranked_by_minute_diff = 1
group by v2.patientunitstayid, pf_lung_offset, v2.pfratio, v2.ARDS_severity, v2.lung_compliance, mortality,gender,bmi,ethnicity,age,TV_IBW_calculated,TV_IBW,peep
order by patientunitstayid,pf_lung_offset


-- 1060


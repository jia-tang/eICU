-- This table selects:
-- combine static compliance and pf ratio, in a 6 hour interval 
-- lung compliance range 7.4-100
-- peep > 5

-- drop table if exists `ync-capstones.Jia.combined_minpf_compliance`;
-- create table `ync-capstones.Jia.combined_minpf_compliance`  as

with v1 as (
select p.patientunitstayid,p.gender,p.bmi,p.ethnicity,p.age, p.fio2_offset, l.chartoffset as lung_offset, p.pfratio, p.groupx as ARDS_severity,l.peep, l.lung_compliance,TV_IBW,p.unitdischargestatus as mortality
,l.tidal_volume/p.IBW_calculated as TV_IBW_calculated
from `ync-capstones.Jia.patient_min_pfratio`  p
inner join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and abs(p.fio2_offset - l.chartoffset) < 60*6 -- combine pfratio and lung compliance in a 6 hour interval
where lung_compliance > 7.4 -- eliminate rows with compliance too low (<e^2) and too high (>100)
and lung_compliance < 100
order by p.patientunitstayid, fio2_offset),

v2 as ( 
select *
--, ROW_NUMBER() OVER (partition by v1.patientunitstayid, lung_offset order by pf_offset ) as firstRowa
--, ROW_NUMBER() OVER (partition by v1.patientunitstayid, cast (pf_offset as numeric) order by lung_offset ) as firstRowb
, ROW_NUMBER() OVER (partition by v1.patientunitstayid order by ABS(fio2_offset-lung_offset) asc) as ranked_by_minute_diff
from v1)

select patientunitstayid,gender,bmi,ethnicity,age, fio2_offset,lung_offset, pfratio, ARDS_severity, lung_compliance, mortality,TV_IBW_calculated,TV_IBW,peep
,CASE 
        WHEN lung_compliance <= 26.47 THEN "S4"
        WHEN lung_compliance between 26.47 and 33.33 THEN "S3"
        WHEN lung_compliance between 33.33 and 44 THEN "S2"
        WHEN lung_compliance >= 44 THEN "S1"
    END AS compliance_level
from v2
where ranked_by_minute_diff = 1
and cast(peep as numeric) >= 5
group by patientunitstayid, pfratio, ARDS_severity, lung_compliance, mortality,gender,bmi,ethnicity,age,TV_IBW_calculated,TV_IBW,peep,fio2_offset,lung_offset
order by patientunitstayid,fio2_offset


--747


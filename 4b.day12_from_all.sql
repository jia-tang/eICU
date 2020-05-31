with v1 as (
select patientunitstayid from `ync-capstones.Jia.combined_allpf_compliance`
where day=1
group by patientunitstayid),

v2 as (
select patientunitstayid from `ync-capstones.Jia.combined_allpf_compliance`
where day=2
group by patientunitstayid),

v3 as (select v1.patientunitstayid,gender,bmi,ethnicity,age, pf_lung_offset, pfratio, ARDS_severity, lung_compliance, mortality, TV_IBW_calculated,TV_IBW,day
,    CASE 
        WHEN lung_compliance <= 26.47 THEN "S4"
        WHEN lung_compliance between 26.47 and 33.33 THEN "S3"
        WHEN lung_compliance between 33.33 and 44 THEN "S2"
        WHEN lung_compliance >= 44 THEN "S1"
    END AS compliance_level
from `ync-capstones.Jia.combined_allpf_compliance` final
inner join v1
on v1.patientunitstayid=final.patientunitstayid
inner join v2 
on v2.patientunitstayid=final.patientunitstayid)

select patientunitstayid
, max ( case when day=1 then compliance_level end ) day1_compliance
, max ( case when day=2 then compliance_level end ) day2_compliance
, max ( case when day=1 then ARDS_severity end ) day1_ARDS
, max ( case when day=2 then ARDS_severity end ) day2_ARDS
from v3
group by patientunitstayid

--424

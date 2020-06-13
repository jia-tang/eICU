with v1 as (
select *
,    CASE 
        WHEN lung_compliance <= 26.47 THEN "S4"
        WHEN lung_compliance between 26.47 and 33.33 THEN "S3"
        WHEN lung_compliance between 33.33 and 44 THEN "S2"
        WHEN lung_compliance >= 44 THEN "S1"
    END AS compliance_level
,ROW_NUMBER() OVER (partition by patientunitstayid order by ABS(chartoffset-0) asc) as ranked_0hour
from `ync-capstones.Jia.lungcompliance`), 

d1 as (
select *
from v1
where abs(chartoffset-0)<24*60
and ranked_0hour=1),

d2 as (
select v1.patientunitstayid,lung_compliance from v1
inner join 
(select v1.patientunitstayid, min(lung_compliance) as min_compliance
from v1 group by patientunitstayid) t
on v1.patientunitstayid=t.patientunitstayid and v1.lung_compliance=t.min_compliance
where abs(chartoffset-0) between 24*60 and 47*60)

select min.patientunitstayid, d1.lung_compliance as day1_compliance
, d2.lung_compliance as day2_compliance from `ync-capstones.Jia.patient_min_pfratio` min
inner join d1 
on d1.patientunitstayid=min.patientunitstayid
inner join d2
on d2.patientunitstayid=min.patientunitstayid
order by patientunitstayid,chartoffset

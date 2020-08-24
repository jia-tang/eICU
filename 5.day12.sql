--1511 patients

with v1 as (
select compliance.*
,ROW_NUMBER() OVER (partition by compliance.patientunitstayid order by ABS(TV_offset-0) asc) as ranked_0hour
from `ync-capstones.Jia.lungcompliance` compliance
inner join `ync-capstones.Jia.combined_firstpf_compliance` pf
on compliance.patientunitstayid=pf.patientunitstayid
), 

d1 as ( -- Day 1 compliance: the first compliance on Day 1
select *
from v1
where abs(TV_offset-0)<24*60
and ranked_0hour=1),

d2 as ( -- Day 2 compliance: the worst compliance on Day 2
select v1.patientunitstayid,lung_compliance from v1
inner join 
(select v1.patientunitstayid, min(lung_compliance) as min_compliance
from v1 
where TV_offset between 24*60 and 47*60
group by patientunitstayid) t
on v1.patientunitstayid=t.patientunitstayid and v1.lung_compliance=t.min_compliance
group by patientunitstayid,lung_compliance
),

final as (
select first.patientunitstayid, d1.lung_compliance as day1
, d2.lung_compliance as day2
 from `ync-capstones.Jia.combined_firstpf_compliance` first
inner join d1 
on d1.patientunitstayid=first.patientunitstayid
inner join d2
on d2.patientunitstayid=first.patientunitstayid
order by patientunitstayid,TV_offset)

select * ,CASE 
        WHEN day1 > 50 THEN "Type L"
        When day1 between 40 and 50 THEN "Intermediate phenotype"
        WHEN day1 <= 40 THEN "Type H"
    END AS day1_compliance
    , CASE 
        WHEN day2 > 50 THEN "Type L"
        When day2 between 40 and 50 THEN "Intermediate phenotype"
        WHEN day2 <= 40 THEN "Type H"
    END AS day2_compliance
from final


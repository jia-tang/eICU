with v1 as (
select *
,ROW_NUMBER() OVER (partition by patientunitstayid order by ABS(chartoffset-0) asc) as ranked_0hour
from `ync-capstones.Jia.lungcompliance`), 

d1 as ( -- Day 1 compliance: the first compliance on Day 1
select *
from v1
where abs(chartoffset-0)<24*60
and ranked_0hour=1),

d2 as ( -- Day 2 compliance: the worst compliance on Day 2
select v1.patientunitstayid,lung_compliance from v1
inner join 
(select v1.patientunitstayid, min(lung_compliance) as min_compliance
from v1 
where chartoffset between 24*60 and 47*60
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
order by patientunitstayid,chartoffset)

select * ,CASE 
        WHEN day1 <= 35 THEN "L"
        WHEN day1 >= 35 THEN "H"
    END AS day1_compliance
    , CASE 
        WHEN day2 <= 35 THEN "L"
        WHEN day2 >= 35 THEN "H"
    END AS day2_compliance
from final
-- 2002 data



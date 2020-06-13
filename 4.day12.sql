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
from v1 group by patientunitstayid) t
on v1.patientunitstayid=t.patientunitstayid and v1.lung_compliance=t.min_compliance
where abs(chartoffset-0) between 24*60 and 47*60)

select min.patientunitstayid, d1.lung_compliance as day1_compliance
, d2.lung_compliance as day2_compliance from `ync-capstones.Jia.combined_minpf_compliance` min
inner join d1 
on d1.patientunitstayid=min.patientunitstayid
inner join d2
on d2.patientunitstayid=min.patientunitstayid
order by patientunitstayid,chartoffset


--102

-- 1926 data with day 1 from lung_compliance
-- 319 data with day 2 from lung_compliance
-- combine with pfratio on day1 patient: 102 data



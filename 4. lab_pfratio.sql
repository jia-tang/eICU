--drop table if exists `ync-capstones.Jia.lab_pfratio`;
--create table `ync-capstones.Jia.lab_pfratio` as
with v1 as(
select patientunitstayid, chartoffset
,fio2
from `ync-capstones.Jia.lab_keys_raw`
where fio2 is not null
order by patientunitstayid,chartoffset),

v2 as (
select patientunitstayid, chartoffset, pao2
from `ync-capstones.Jia.lab_keys_raw`
where pao2 is not null
order by patientunitstayid,chartoffset),

v3 as (
select v1.patientunitstayid, v1.chartoffset as pf_offset, v2.pao2, v1.fio2
from v1
inner join v2 
on v1.patientunitstayid=v2.patientunitstayid
and v1.chartoffset = v2.chartoffset), -- didn't use 1h interval, as doesn't increase much data, and may result in repetitive data

v4 as (
select *, pao2/fio2 as pfratio
from v3)

SELECT *, 
    CASE 
        WHEN pfratio <= 100 THEN "severe"
        WHEN pfratio between 100 and 200 THEN "moderate"
        WHEN pfratio between 200 and 300 THEN "mild"
    END AS groupx
FROM v4
where pfratio<=300
order by patientunitstayid, pf_offset

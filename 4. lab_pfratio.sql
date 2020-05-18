drop table if exists `ync-capstones.Jia.lab_pfratio`;
create table `ync-capstones.Jia.lab_pfratio` as
with v1 as(
select patientunitstayid, chartoffset
,fio2, pao2
, pao2/fio2 as pfratio
, round(chartoffset/60) as hr1, round(chartoffset/180) as hr3, round(chartoffset/300) as hr5, round(chartoffset/420) as hr7
, round(chartoffset/540) as hr9, round(chartoffset/720) h12, round(chartoffset/1440) h24
from `ync-capstones.Jia.lab_keys_raw`
where pao2 is not null
and fio2 is not null
order by patientunitstayid,chartoffset)

SELECT *, 
    CASE 
        WHEN pfratio <= 100 THEN "severe"
        WHEN pfratio between 100 and 200 THEN "moderate"
        WHEN pfratio between 200 and 300 THEN "mild"
    END AS groupx
FROM v1
where pfratio<=300


drop table if exists `ync-capstones.Jia.lungcompliance`;

create table `ync-capstones.Jia.lungcompliance` as 

with v1 as(
select patientunitstayid, chartoffset
, cast(Tidal_Volume as numeric) as tidal_volume
,  cast(Plateau_Pressure as numeric) as plateau_pressure
,  cast(peep as numeric) as peep
, round(chartoffset/60) as hr1, round(chartoffset/180) as hr3, round(chartoffset/300) as hr5, round(chartoffset/420) as hr7
, round(chartoffset/540) as hr9, round(chartoffset/720) h12, round(chartoffset/1440) h24
from `ync-capstones.Jia.lungcompliance_raw`)


select *, tidal_volume/nullif((plateau_pressure-peep),0) as lung_compliance from v1
where tidal_volume is not null
and plateau_pressure is not null
and peep is not null
order by patientunitstayid, chartoffset

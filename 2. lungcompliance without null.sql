drop view if exists `ync-capstones.Jia.lungcompliance`;

create view `ync-capstones.Jia.lungcompliance` as 

with v1 as(
select patientunitstayid, chartoffset
, cast(Tidal_Volume as numeric) as tidal_volume
,  cast(Plateau_Pressure as numeric) as plateau_pressure
,  cast(peep as numeric) as peep
from `ync-capstones.Jia.lungcompliance_raw`)


select *, tidal_volume/nullif((plateau_pressure-peep),0) as lung_compliance from v1
where tidal_volume is not null
and plateau_pressure is not null
and peep is not null
order by patientunitstayid, chartoffset

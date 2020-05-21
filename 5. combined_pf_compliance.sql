-- combine static compliance and pf ratio, in a 6 hour interval (gonna check with KC if it's okay).
-- lung compliance range 0-300 (to be checked)

drop table if exists `ync-capstones.Jia.combined_pf_compliance`;
create table `ync-capstones.Jia.combined_pf_compliance`  as
with v1 as (
select p.patientunitstayid, p.pf_offset, l.chartoffset as lung_offset, p.pfratio, p.groupx as ARDS_severity, l.lung_compliance
from `ync-capstones.Jia.lab_pfratio` p
right join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and abs(p.pf_offset - l.chartoffset) < 60*6 -- combine pfratio and lung compliance in a 6 hour interval
where pfratio is not null
and lung_compliance > 0
and lung_compliance < 300
order by p.patientunitstayid, pf_offset),

v2 as ( 
select *
, ROW_NUMBER() OVER (partition by v1.patientunitstayid, lung_offset order by pf_offset ) as firstRow
from v1)

select v2.patientunitstayid, v2.pf_offset, v2.lung_offset, v2.pfratio, v2.ARDS_severity, v2.lung_compliance, m.actualhospitalmortality as mortality -- add mortality
from `physionet-data.eicu_crd.apachepatientresult` m
inner join v2
on v2.patientunitstayid=m.patientunitstayid
where v2.firstRow=1
group by v2.patientunitstayid, v2.pf_offset, v2.lung_offset, v2.pfratio, v2.ARDS_severity, v2.lung_compliance, mortality
order by patientunitstayid





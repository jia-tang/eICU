-- combine static compliance and pf ratio, in a 12 hour interval (gonna check with KC if it's okay).
-- lung compliance range 0-300 (to be checked)

drop table if exists `ync-capstones.Jia.combined_pf_compliance`;
create table  `ync-capstones.Jia.combined_pf_compliance`   as
select p.patientunitstayid, p.chartoffset as p_chartoffset ,l.chartoffset as l_chartoffset, p.pfratio, p.groupx as ARDS_severity, l.lung_compliance
from `ync-capstones.Jia.lab_pfratio` p
right join `ync-capstones.Jia.lungcompliance` l
on p.patientunitstayid=l.patientunitstayid
and p.h12 = l.h12
where pfratio is not null
and lung_compliance>0
and lung_compliance<300
order by p.patientunitstayid, p_chartoffset



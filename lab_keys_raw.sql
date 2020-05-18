DROP TABLE IF EXISTS `ync-capstones.Jia.lab_keys_raw`;
CREATE TABLE `ync-capstones.Jia.lab_keys_raw` as
-- get blood gas measures
with vw0 as
(
  select
      patientunitstayid
    , labname
    , labresultoffset
    , labresultrevisedoffset
  from `physionet-data.eicu_crd.lab`
  where labname in
  (
        'paO2'
      , 'FiO2'
      , 'TV'
      , 'Peak Airway/Pressure'
      , 'PEEP'
  )
  group by patientunitstayid, labname, labresultoffset, labresultrevisedoffset
  having count(distinct labresult)<=1
)
-- get the last lab to be revised
, vw1 as
(
  select
      lab.patientunitstayid
    , lab.labname
    , lab.labresultoffset
    , lab.labresultrevisedoffset
    , lab.labresult
    , ROW_NUMBER() OVER
        (
          PARTITION BY lab.patientunitstayid, lab.labname, lab.labresultoffset
          ORDER BY lab.labresultrevisedoffset DESC
        ) as rn
  from `physionet-data.eicu_crd.lab` as lab
  inner join vw0
    ON  lab.patientunitstayid = vw0.patientunitstayid
    AND lab.labname = vw0.labname
    AND lab.labresultoffset = vw0.labresultoffset
    AND lab.labresultrevisedoffset = vw0.labresultrevisedoffset
  WHERE
     (lab.labname = 'paO2' and lab.labresult >= 15 and lab.labresult <= 720)
  OR (lab.labname = 'FiO2' and lab.labresult >= 0.2 and lab.labresult <= 1.0)
  -- we will fix fio2 units later
  OR (lab.labname = 'FiO2' and lab.labresult >= 20 and lab.labresult <= 100)
  OR (lab.labname = 'TV' )
  OR (lab.labname = 'Peak Airway/Pressure')
  OR (lab.labname = 'PEEP' and lab.labresult >= 0 and lab.labresult <= 60)
)
select
    patientunitstayid
  , labresultoffset as chartoffset
  -- the aggregate (max()) only ever applies to 1 value due to the where clause
  , MAX(case
        when labname != 'FiO2' then null
        when labresult >= 20 then labresult/100.0
      else labresult end) as fio2
  , MAX(case when labname = 'paO2' then labresult else null end) as pao2
  , MAX(case when labname = 'TV' then labresult else null end) as TV
  , MAX(case when labname = 'Peak Airway/Pressure' then labresult else null end) as Peak_AirwayPressure
  , MAX(case when labname = 'PEEP' then labresult else null end) as peep
from vw1
where rn = 1
group by patientunitstayid, labresultoffset
order by patientunitstayid, labresultoffset;

drop view if exists `ync-capstones.Jia.lungcompliance_raw`;
create view `ync-capstones.Jia.lungcompliance_raw` as
with vw0 as
(
  select
      patientunitstayid
    , respchartvaluelabel
    , respchartoffset
  from `physionet-data.eicu_crd.respiratorycharting`
  where respchartvaluelabel in
  ( 'PEEP'
      , 'Plateau Pressure'
      , 'Tidal Volume Observed (VT)'
      ,'Static Compliance'
  )
  group by patientunitstayid, respchartvaluelabel, respchartoffset
)

, vw1 as
(
  select
      respiratorycharting.patientunitstayid
    , respiratorycharting.respchartvaluelabel
    , respiratorycharting.respchartoffset
    , respiratorycharting.respchartvalue
    , ROW_NUMBER() OVER
        (
          PARTITION BY respiratorycharting.patientunitstayid, respiratorycharting.respchartvaluelabel, respiratorycharting.respchartoffset
          ORDER BY respiratorycharting.respchartoffset DESC
        ) as rn
  from `physionet-data.eicu_crd.respiratorycharting` respiratorycharting
  inner join vw0
    ON  respiratorycharting.patientunitstayid = vw0.patientunitstayid
    AND respiratorycharting.respchartvaluelabel = vw0.respchartvaluelabel
    AND respiratorycharting.respchartoffset = vw0.respchartoffset
  WHERE
     (respiratorycharting.respchartvaluelabel = 'Plateau Pressure')
  OR (respiratorycharting.respchartvaluelabel = 'PEEP' )
  OR (respiratorycharting.respchartvaluelabel = 'Tidal Volume Observed (VT)')
  OR (respiratorycharting.respchartvaluelabel = 'Static Compliance' )
)

select
    patientunitstayid
  , respchartoffset as chartoffset
  -- the aggregate (max()) only ever applies to 1 value due to the where clause
  , max (case when respchartvaluelabel = 'Plateau Pressure' then respchartvalue else null end )as Plateau_Pressure
  , max(case when respchartvaluelabel = 'Tidal Volume Observed (VT)' then respchartvalue else null end )as Tidal_Volume
  , max(case when respchartvaluelabel = 'Static Compliance' then respchartvalue else null end )as Static_Compliance
  , max(case when respchartvaluelabel = 'PEEP' then respchartvalue else null end )as peep
from vw1
where rn = 1
group by patientunitstayid, respchartoffset
order by patientunitstayid, respchartoffset;

drop table if exists `ync-capstones.Jia.lungcompliance`;
create table `ync-capstones.Jia.lungcompliance` as
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
      ,'TV/kg IBW'
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
  OR (respiratorycharting.respchartvaluelabel = 'TV/kg IBW' )
),

vw2 as(
select
    patientunitstayid
  , cast(respchartoffset as numeric) as chartoffset
  -- the aggregate (max()) only ever applies to 1 value due to the where clause
  , max (case when respchartvaluelabel = 'Plateau Pressure' then respchartvalue else null end )as Plateau_Pressure
  , max(case when respchartvaluelabel = 'Tidal Volume Observed (VT)' then respchartvalue else null end )as Tidal_Volume
  , max(case when respchartvaluelabel = 'Static Compliance' then respchartvalue else null end )as Static_Compliance
  , max(case when respchartvaluelabel = 'PEEP' then respchartvalue else null end )as peep
  , max(case when respchartvaluelabel = 'TV/kg IBW' then respchartvalue else null end )as TV_IBW
from vw1
where rn = 1
group by patientunitstayid, respchartoffset
order by patientunitstayid, respchartoffset),

v1 as(
select patientunitstayid, chartoffset
, cast(Tidal_Volume as numeric) as tidal_volume
,  cast(Plateau_Pressure as numeric) as plateau_pressure
,  cast(peep as numeric) as peep
,  cast(TV_IBW as numeric) as TV_IBW
from vw2)

select *,tidal_volume/nullif((plateau_pressure-peep),0)as lung_compliance 
from v1
where tidal_volume is not null
and plateau_pressure is not null
and peep is not null
order by patientunitstayid, chartoffset -- no need interval, because peep and tidal volume r in almost all rows, but static compliance r few

-- 25208 data, 2213 patients

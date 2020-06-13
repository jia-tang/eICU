-- This table
-- Combine TV,peep,plateau pressure
  -- peep has most data, so to maximize sample size: combine TV with peep with a 2 hour interval; combine plateau pressure with a 2 hour interval

--drop table if exists `ync-capstones.Jia.lungcompliance`;
--create table `ync-capstones.Jia.lungcompliance` as
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
  , max(case when respchartvaluelabel = 'Static Compliance' then respchartvalue else null end )as Static_Compliance
  , max(case when respchartvaluelabel = 'PEEP' then respchartvalue else null end )as peep
  , max(case when respchartvaluelabel = 'TV/kg IBW' then respchartvalue else null end )as TV_IBW
from vw1
where rn = 1
group by patientunitstayid, respchartoffset
order by patientunitstayid, respchartoffset),

TV as(
select
    patientunitstayid
  , cast(respchartoffset as numeric) as chartoffset
  , max(case when respchartvaluelabel = 'Tidal Volume Observed (VT)' then respchartvalue else null end )as tidal_volume
from vw1
where rn = 1
group by patientunitstayid, respchartoffset
order by patientunitstayid, respchartoffset),

Plateau as(
select
    patientunitstayid
  , cast(respchartoffset as numeric) as chartoffset
  , max(case when respchartvaluelabel = 'Plateau Pressure' then respchartvalue else null end )as plateau_pressure
from vw1
where rn = 1
group by patientunitstayid, respchartoffset
order by patientunitstayid, respchartoffset),

v1 as(
select vw2.patientunitstayid, vw2.chartoffset
, cast(tidal_volume as numeric) as tidal_volume
, cast(plateau_pressure as numeric) as plateau_pressure
, cast(peep as numeric) as peep
, cast(TV_IBW as numeric) as TV_IBW
, ROW_NUMBER() OVER (partition by vw2.patientunitstayid,vw2.chartoffset order by ABS(TV.chartoffset-vw2.chartoffset) asc) as ranked_tv_diff
, ROW_NUMBER() OVER (partition by vw2.patientunitstayid,vw2.chartoffset order by ABS(TV.chartoffset-vw2.chartoffset) asc) as ranked_pp_diff
from vw2
inner join TV 
on TV.patientunitstayid=vw2.patientunitstayid
inner join Plateau
on Plateau.patientunitstayid=vw2.patientunitstayid
where 
abs (TV.chartoffset-vw2.chartoffset)<2*60
and abs (Plateau.chartoffset-vw2.chartoffset)<2*60
and plateau_pressure is not null
and peep is not null
and tidal_volume is not null)


select v1.*,tidal_volume /nullif(plateau_pressure- peep,0)as lung_compliance
from v1
where ranked_tv_diff=1
and ranked_pp_diff=1
order by patientunitstayid, chartoffset -- no need interval, because peep and tidal volume r in almost all rows, but plateau_pressure r few

-- 25208 data, 2213 patients, if match with exact time
-- 28124 data, with a 2 hour interval allowed for Tidal volume with peep.
-- 30274 data, with a 2 hour interval allowed for plateau pressure with peep.


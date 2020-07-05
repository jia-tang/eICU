-- This table
-- Combine TV (set <2000),peep,plateau pressure
  -- Match the 3 values at the same time stamp (allow 2-hour interval doesn't increase sample size)  
-- Extract max peep in the first 24 hours
-- eliminate rows with compliance too low (<e^2) and too high (>100)

drop view if exists `ync-capstones.Jia.lungcompliance`;
create view `ync-capstones.Jia.lungcompliance` as
with vw0 as
(
  select
      patientunitstayid
    , respchartvaluelabel
    , safe_cast(respchartoffset as numeric) as chartoffset
    , safe_cast(respchartvalue as numeric) as respchartvalue
  from `physionet-data.eicu_crd.respiratorycharting`),
  
vw2 as(
select
    patientunitstayid
  , chartoffset
  -- the aggregate (max()) only ever applies to 1 value due to the where clause
  , case when respchartvaluelabel = 'Static Compliance' then respchartvalue else null end as static_compliance
  , case when respchartvaluelabel in( 'PEEP','PEEP/CPAP') then respchartvalue else null end as peep
  , case when respchartvaluelabel = 'TV/kg IBW' then respchartvalue else null end as TV_IBW
from vw0
order by patientunitstayid, chartoffset),

TV as( -- 497307 data
select
    patientunitstayid
  , chartoffset
  , case 
  when respchartvalue > 0 and respchartvalue <= 1 then respchartvalue*1000 -- may be recorded as L, not mL
  when respchartvalue > 10 and respchartvalue<=2000 then respchartvalue 
  else null end as tidal_volume
from vw0
where respchartvaluelabel in ('Tidal Volume Observed (VT)','Exhaled TV (patient)','Exhaled Vt','Tidal Volume (set)','Tidal Volume, Delivered','Spont TV','Set Vt (Servo,LTV)','Exhaled TV (machine)')
order by patientunitstayid, chartoffset
),

first_TV as (
select 
    patientunitstayid
  , tidal_volume as first_tv
  from (
  select TV.patientunitstayid, tidal_volume,chartoffset,
  ROW_NUMBER() OVER (partition by TV.patientunitstayid order by tidal_volume asc) as rank_tv
  from TV
) 
  where rank_tv=1
),

Plateau as(
select
    patientunitstayid
  , chartoffset
  , case when respchartvaluelabel = 'Plateau Pressure' then respchartvalue else null end as plateau_pressure
from vw0
order by patientunitstayid, chartoffset),

MaxPeep as ( -- max peep in the first 24 hour
select 
    patientunitstayid
  , peep as max_peep
  from (
  select vw2.patientunitstayid, peep,chartoffset,
  ROW_NUMBER() OVER (partition by vw2.patientunitstayid order by peep desc) as rank_peep
  from vw2
  where chartoffset <=24*60
) 
  where rank_peep=1
),

TV_IBW as(
select
    patientunitstayid
    ,TV_IBW
from (
select vw2.patientunitstayid, TV_IBW, chartoffset,
  ROW_NUMBER() OVER (partition by vw2.patientunitstayid order by chartoffset desc) as rank_tvibw
  from vw2)
  where rank_tvibw=1
order by patientunitstayid, chartoffset),

v1 as(
select vw2.patientunitstayid, vw2.chartoffset, tidal_volume, plateau_pressure, peep,TV_IBW.TV_IBW, max_peep, first_TV.first_tv
, ROW_NUMBER() OVER (partition by vw2.patientunitstayid,vw2.chartoffset order by ABS(TV.chartoffset-vw2.chartoffset) asc) as ranked_tv_diff
, ROW_NUMBER() OVER (partition by vw2.patientunitstayid,vw2.chartoffset order by ABS(TV.chartoffset-vw2.chartoffset) asc) as ranked_pp_diff
from vw2
inner join TV 
on TV.patientunitstayid=vw2.patientunitstayid
inner join Plateau
on Plateau.patientunitstayid=vw2.patientunitstayid
left join MaxPeep
on MaxPeep.patientunitstayid=vw2.patientunitstayid
left join first_TV
on first_TV.patientunitstayid=vw2.patientunitstayid
left join TV_IBW
on TV_IBW.patientunitstayid=vw2.patientunitstayid
where 
abs (TV.chartoffset-vw2.chartoffset)<=4*60
and abs (Plateau.chartoffset-vw2.chartoffset)<=4*60
and plateau_pressure is not null
and peep is not null
and tidal_volume is not null),


final as
(select v1.*,tidal_volume /nullif(plateau_pressure- peep,0)as lung_compliance
from v1
where ranked_tv_diff=1
and ranked_pp_diff=1
order by patientunitstayid, chartoffset)

select * from final
where lung_compliance > 7.4 -- eliminate rows with compliance too low (<e^2) and too high (>100)
and lung_compliance < 100

-- 530387 data, 22436 patients 

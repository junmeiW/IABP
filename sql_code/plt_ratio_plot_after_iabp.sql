-- 绘制 plt——ratio 随时间变化图
with first_iabp as (
-- 这里获得IABP患者
	SELECT distinct icustay_id
     , age
		 , gender
		 , weight
		 , height
		 , icu_los_hours
		 , icutype
		 , unit_mort
		 , hosp_mort
		 , hosp_los
     , treatmentoffset
		 , last_plt
		 , min_plt
		 , plt_dpc
 from IABP_hist
)
, platelet as (
-- 血红蛋白  hgb
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult 
				 , labmeasurenamesystem
				 , labmeasurenameinterface
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like '%platelet%'
		and labresult is not null
)
, plt_afterbase as (
-- iabp 后的plt测量值
  SELECT fi.icustay_id
	   , fi.age
		 , fi.gender
		 , fi.weight
		 , fi.height
		 , fi.icu_los_hours
		 , fi.icutype
		 , fi.last_plt
		 , fi.plt_dpc
     , fi.treatmentoffset
-- 		 , fi.treatmentoffset_hour
		 , pt.labresultoffset
		 , pt.labresult as plt_aftiabp
	 from first_iabp fi
	 inner join platelet pt
	 on fi.icustay_id = pt.icustay_id
	 where pt.labresultoffset >= fi.treatmentoffset
)
, tmp_plt as (
SELECT  icustay_id
-- 	   ,  age
-- 		 ,  gender
-- 		 ,  weight
-- 		 ,  height
-- 		 ,  icu_los_hours
		 ,  icutype
     ,  treatmentoffset
		 ,  last_plt
		 ,  plt_dpc
		 , case when plt_dpc <= 0.25 then 1
		        when (plt_dpc > 0.25  and plt_dpc <= 0.5) then 2
						when (plt_dpc > 0.5  and plt_dpc <= 0.75) then 3
						when (plt_dpc > 0.75  and plt_dpc <= 1.0) then 4
				 else null
			 end as dpc_group
		 , round((plt_aftiabp/last_plt),3) as plt_ratio -- 该值为iabp后的plt测量值与基线值的比
		 ,  round((labresultoffset/60),4) as labresultoffsetH
		--  , round((labresultoffset::NUMERIC/60::NUMERIC/24::NUMERIC),5) as labresultoffset_day
		 ,  plt_aftiabp

from plt_afterbase
)
, tmp_plt2 as (
  SELECT icustay_id
		 ,  plt_dpc
		 , dpc_group
		 , plt_ratio -- 该值为iabp后的plt测量值与基线值的比
		 , labresultoffsetH
		 , round((labresultoffsetH::NUMERIC/24::NUMERIC),5) as labresultoffset_day
		 , ceil((labresultoffsetH::NUMERIC/24::NUMERIC)) as day_offset
from tmp_plt
)
, day_off as (
select distinct icustay_id,  day_offset from tmp_plt2
)
SELECT count(*) from day_off
group by day_offset
order by day_offset

;
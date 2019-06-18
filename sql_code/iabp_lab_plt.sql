DROP MATERIALIZED VIEW IF EXISTS IABP_plt CASCADE;
CREATE MATERIALIZED VIEW IABP_plt as (
with first_iabp1 as (
-- 这里获得IABP患者首次插管的时间
SELECT * from 
	(
	SELECT icustay_id
	   , age
		 , gender
		 , admissionweight as weight
		 , admissionheight as height
		 , icu_los_hours
		 , icutype
		 , unit_mort
		 , hosp_mort
     , treatmentoffset
		 , treatmentoffset_hour
		 , "row_number"() over(PARTITION by icustay_id ORDER BY treatmentoffset) as first_ip
	from IABP_detail
 )tmp
 where first_ip = 1 
)
, first_iabp as (
-- 这里获得IABP患者 hosp_los
	SELECT icustay_id
	   , age
		 , gender
		 , weight
		 , height
		 , icu_los_hours
		 , icutype
		 , unit_mort
		 , hosp_mort
     , treatmentoffset
		 , treatmentoffset_hour
		 , ar.actualhospitallos as hosp_los
	from first_iabp1 f1
	left join apachepatientresult ar
	on f1.icustay_id = ar.patientunitstayid

)
, platelet as (
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
, plt_iabp as (
  SELECT fi.icustay_id
     , fi.treatmentoffset
		 , fi.treatmentoffset_hour
		 , pt.labresultoffset
		 , pt.labresult
		 , labmeasurenamesystem
		 , labmeasurenameinterface
	 from first_iabp fi
	 inner join platelet pt
	 on fi.icustay_id = pt.icustay_id
	 
)
-- SELECT distinct icustay_id from plt_iabp  -- 986(not null)  
, last_plt as (
-- 获得 第一次插入IABP时间之前最近的plt值
SELECT * from (
 SELECT  icustay_id
     , treatmentoffset
		 , treatmentoffset_hour
		 , labresultoffset
		 , labresult
-- 		 , labmeasurenamesystem
-- 		 , labmeasurenameinterface
		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresultoffset desc) as lastplt
 from plt_iabp
 where labresultoffset <= treatmentoffset
) lp
where lastplt = 1
)
--  SELECT distinct icustay_id from last_plt -- 835 
, lowerplt as (
-- 剔除plt值小于100的患者
SELECT icustay_id
     , treatmentoffset
		 , labresultoffset
		 , labresult as last_plt
-- 		 , labmeasurenamesystem
-- 		 , labmeasurenameinterface
		 from last_plt
-- 	 where labresult is null  11
 where labresult >= 100
)
, minplt as (
 SELECT * from (
   SELECT  icustay_id
				, treatmentoffset
				, treatmentoffset_hour
				, labresultoffset
				, labresult
-- 				, labmeasurenamesystem
-- 		 , labmeasurenameinterface
		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresult ) as min_plt1
 from plt_iabp
 where labresultoffset >= treatmentoffset
) lp
where min_plt1 = 1
)
, plt_cal as (
SELECT lp.icustay_id
		 , de.age
		 , de.gender
		 , de.weight
		 , de.height
		 , de.icutype
		 , de.icu_los_hours
		 , de.unit_mort
		 , de.hosp_mort 
		 , de.hosp_los
     , lp.treatmentoffset
		 , last_plt
		 , mp.labresult as min_plt
 from lowerplt lp
 inner join minplt mp
     on lp.icustay_id = mp.icustay_id
		 and lp.treatmentoffset = mp.treatmentoffset
 left join first_iabp de
  on lp.icustay_id = de.icustay_id
	and lp.treatmentoffset = de.treatmentoffset
)
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
		 , round((last_plt - min_plt)/last_plt, 4) as dpc
 from plt_cal

-- where height is null
)







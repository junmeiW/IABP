DROP MATERIALIZED VIEW IF EXISTS IABP_basic1 CASCADE;
CREATE MATERIALIZED VIEW IABP_basic1 as (
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
		 , dpc
 from IABP_plt
)
, hgb_get as (
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
		and lower(labname) like 'hgb'
		and labresult is not null
)
, hgb_iabp as (
  SELECT fi.icustay_id
     , fi.treatmentoffset
-- 		 , fi.treatmentoffset_hour
		 , pt.labresultoffset
		 , pt.labresult
		 , labmeasurenamesystem
		 , labmeasurenameinterface
	 from first_iabp fi
	 inner join hgb_get pt
	 on fi.icustay_id = pt.icustay_id
	 
)
-- SELECT distinct icustay_id from plt_iabp  -- 986(not null)  
, last_hgb1 as (
-- 获得 第一次插入IABP时间之前最近的plt值
SELECT * from (
 SELECT  icustay_id
     , treatmentoffset
-- 		 , treatmentoffset_hour
		 , labresultoffset
		 , labresult as last_hgb
-- 		 , labmeasurenamesystem
-- 		 , labmeasurenameinterface
		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresultoffset desc) as lasthgb
 from hgb_iabp
 where labresultoffset <= treatmentoffset
) lp
where lasthgb = 1
)
--  SELECT distinct icustay_id from last_plt -- 835 

, minhgb as (
 SELECT * from (
   SELECT  icustay_id
				, treatmentoffset
-- 				, treatmentoffset_hour
				, labresultoffset
				, labresult as min_hgb
-- 				, labmeasurenamesystem
-- 		 , labmeasurenameinterface
		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresult ) as min_hgb1
 from hgb_iabp
 where labresultoffset >= treatmentoffset
) lp
where min_hgb1 = 1
)
, hgb_cal as (
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
		 , last_hgb
		 , min_hgb
 from last_hgb1 lp
 inner join minhgb mp
     on lp.icustay_id = mp.icustay_id
		 and lp.treatmentoffset = mp.treatmentoffset
 left join first_iabp de
  on lp.icustay_id = de.icustay_id
	and lp.treatmentoffset = de.treatmentoffset
)
, hgb_cal1 as (
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
		 , last_hgb
		 , min_hgb
		 , round((last_hgb - min_hgb),4) as hgb_drop
 from hgb_cal
)

, creatine as (
SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult
				 , labmeasurenamesystem
				 , labmeasurenameinterface
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'creatinine'
		and labresult is not null
)
, crea_iabp as (
  SELECT fi.icustay_id
     , fi.treatmentoffset
-- 		 , fi.treatmentoffset_hour
		 , pt.labresultoffset
		 , pt.labresult
		 , labmeasurenamesystem

	 from first_iabp fi
	 inner join creatine pt
	 on fi.icustay_id = pt.icustay_id 
)
, last_creati as (
-- 获得 第一次插入IABP时间之前最近的creatinine值
SELECT * from (
 SELECT  icustay_id
     , treatmentoffset
		 , labresultoffset
		 , labresult as last_cr
		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresultoffset desc) as lasthgb
 from crea_iabp
 where labresultoffset <= treatmentoffset
) lp
where lasthgb = 1
)
--  SELECT distinct icustay_id from last_plt -- 835 
, maxplt as (
 SELECT * from (
   SELECT  icustay_id
				, treatmentoffset
-- 				, treatmentoffset_hour
				, labresultoffset
				, labresult as max_cr

		 , "row_number"() over(PARTITION by icustay_id, treatmentoffset ORDER BY labresult desc ) as max_plt1
 from crea_iabp
 where labresultoffset >= treatmentoffset
) lp
where max_plt1 = 1
)
, cr_cal as (
SELECT lc.icustay_id
		 , de.age
		 , de.gender
		 , de.weight
		 , de.height
		 , de.icu_los_hours
		 , de.icutype
		 , de.unit_mort
		 , de.hosp_mort 
		 , de.hosp_los
     , lc.treatmentoffset
		 , last_cr
		 , max_cr
 from first_iabp de
 left join last_creati lc
		on lc.icustay_id = de.icustay_id
		and lc.treatmentoffset = de.treatmentoffset
 left  join maxplt mp
     on de.icustay_id = mp.icustay_id
		 and de.treatmentoffset = mp.treatmentoffset
)
, cr_cal1 as (
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
		 , last_cr
		 , max_cr
		 , round((max_cr - last_cr)/last_cr,4) as cr_rise
 from cr_cal
)
, transfusion_rbc as (
 SELECT distinct rbc.icustay_id , rbc.treatmentoffset
 from 
 (
  select patientunitstayid as icustay_id,
       treatmentid,
	   treatmentyear,
	   -- treatmenttime24, 
	   treatmentoffset,
	   round((treatmentoffset::numeric/60::numeric),2) as treatmentoffset_hour,
-- 	   round((treatmentoffset::numeric/60/24::numeric),2) as treatmentoffset_day,
	   treatmentstring,
	   activeupondischarge as active
   from treatment 
   where lower(treatmentstring) like '%packed red blood cells%'
	 )rbc 
	 inner join  first_iabp f
	 on rbc.icustay_id = f.icustay_id 
	 and rbc.treatmentoffset >= f.treatmentoffset
)
, time_rbc as (
SELECT icustay_id
	      , treatmentoffset as rbc_time
from (
  SELECT  icustay_id
	      , treatmentoffset
				, "row_number"() over(PARTITION by icustay_id ORDER BY treatmentoffset) as firstrbc

	from transfusion_rbc
	) fr
	where firstrbc =1 
)
, plt_con as(
	SELECT distinct p.icustay_id, p.treatmentoffset from 
 (
  select patientunitstayid as icustay_id,
       treatmentid,
	   treatmentyear,
	   -- treatmenttime24, 
	   treatmentoffset,
-- 	   round((treatmentoffset::numeric/60::numeric),2) as treatmentoffset_hour,
-- 	   round((treatmentoffset::numeric/60/24::numeric),2) as treatmentoffset_day,
	   treatmentstring,
	   activeupondischarge as active
   from treatment 
   where lower(treatmentstring) like '%platelet concentrate%'
	 )p 
	 inner join  first_iabp f
	 on p.icustay_id = f.icustay_id 
	 and p.treatmentoffset >= f.treatmentoffset
)
, time_pltcon as (
SELECT icustay_id
	      , treatmentoffset as plt_time
from (
  SELECT  icustay_id
	      , treatmentoffset
				, "row_number"() over(PARTITION by icustay_id ORDER BY treatmentoffset) as firstrbc

	from plt_con
	) fr
	where firstrbc =1 
)

, vent as (
SELECT distinct v.icustay_id, v.treatmentoffset from 
	(
  SELECT patientunitstayid as icustay_id, treatmentoffset
	from treatment
	where lower(treatmentstring) SIMILAR TO  '%mechanical ventilation%|%cpap/peep therapy%' 
	and lower(treatmentstring) not like '%mask%'
	and lower(treatmentstring)  not like'%non-invasive ventilation%'
	and lower(treatmentstring)  not like'%surgery%'
  )v
	inner join  IABP_plt f
	 on v.icustay_id = f.icustay_id 
	 and v.treatmentoffset >= f.treatmentoffset
)

-- 将iabp 之前用 机械通气的患者去除
, remove_vent as (
	SELECT distinct v.icustay_id  from 
	(
  SELECT patientunitstayid as icustay_id, treatmentoffset
	from treatment
	where lower(treatmentstring) SIMILAR TO  '%mechanical ventilation%|%cpap/peep therapy%' 
	and lower(treatmentstring) not like '%mask%'
	and lower(treatmentstring)  not like'%non-invasive ventilation%'
	and lower(treatmentstring)  not like'%surgery%'
  )v
	inner join  IABP_plt f
	 on v.icustay_id = f.icustay_id 
	 and v.treatmentoffset < f.treatmentoffset


)
, vent1 as (
 SELECT   icustay_id,  treatmentoffset from vent
 where icustay_id not in (
    SELECT   icustay_id  from remove_vent
 )
)
, time_vent as (
SELECT icustay_id
	      , treatmentoffset as vent_time
from (
  SELECT  icustay_id
	      , treatmentoffset
				, "row_number"() over(PARTITION by icustay_id ORDER BY treatmentoffset) as firstrbc

	from vent1
	) fr
	where firstrbc =1 
)
, diaysis as (
SELECT distinct d.icustay_id, d.treatmentoffset from 
	(
	SELECT patientunitstayid as icustay_id,treatmentoffset
	from treatment
	where lower(treatmentstring) SIMILAR TO  '%dialysis%'
  )d
	inner join  IABP_plt f
	 on d.icustay_id = f.icustay_id 
	 and d.treatmentoffset >= f.treatmentoffset
)
, remove_diaysis as (
SELECT distinct d.icustay_id  from 
	(
	SELECT patientunitstayid as icustay_id,treatmentoffset
	from treatment
	where lower(treatmentstring) SIMILAR TO  '%dialysis%'
  )d
	inner join  IABP_plt f
	 on d.icustay_id = f.icustay_id 
	 and d.treatmentoffset < f.treatmentoffset

)
, diaysis1 as(
SELECT   icustay_id,  treatmentoffset from diaysis
 where icustay_id not in (
    SELECT icustay_id from remove_diaysis
 )

)
, time_diaysis as (
SELECT icustay_id
	      , treatmentoffset as diaysis_time
from (
  SELECT  icustay_id
	      , treatmentoffset
				, "row_number"() over(PARTITION by icustay_id ORDER BY treatmentoffset) as firstrbc

	from diaysis1
	) fr
	where firstrbc =1 
)

-- 需要提的新数据
, sbp_data as(
SELECT distinct icustay_id, observationoffset, sbp 
from (
	select patientunitstayid as icustay_id
	      , observationoffset
				, systemicsystolic as sbp
	from vitalperiodic vp
	where patientunitstayid in (
	 SELECT icustay_id from IABP_plt
	)
	union (
	select patientunitstayid as icustay_id
	      , observationoffset
				, noninvasivesystolic as sbp
	from vitalaperiodic vap
	where patientunitstayid in (
	 SELECT icustay_id from IABP_plt
	)
	)
	)t
	where sbp is not null 
)
, sbp1 as (
-- baseline
 select s.icustay_id, s.sbp
      , s.observationoffset
			, "row_number"() over(PARTITION by s.icustay_id ORDER BY s.observationoffset desc) as last_sbp
	from sbp_data s
	inner join  IABP_plt f
	 on s.icustay_id = f.icustay_id 
	 and s.observationoffset <= f.treatmentoffset
)
, get_sbp as (
  SELECT distinct icustay_id
	     , sbp
			 , observationoffset as sbptime
 from sbp1
 where last_sbp = 1

)
-- , dbp1 as(
--  select s.icustay_id, s.dbp
--       , s.observationoffset
-- 			, "row_number"() over(PARTITION by s.icustay_id ORDER BY s.observationoffset desc) as lastn_dbp
--  from  
--  (
--   select patientunitstayid as icustay_id
-- 	      , observationoffset
-- 				, systemicdiastolic as dbp
-- 	from vitalperiodic vp
-- 	)s
-- 	inner join  IABP_plt f
-- 	 on s.icustay_id = f.icustay_id 
-- 	 and s.observationoffset <= f.treatmentoffset

-- )
-- , get_dbp as (
--  SELECT icustay_id
-- 	     , dbp as last_dbp
-- -- 			 , observationoffset as dbptime
--  from dbp1
--  where lastn_dbp = 1
--  group by icustay_id, dbp
-- )

, total_chole1 as (
 SELECT t.icustay_id, t.total_cholesterol,t.tctime
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.tctime desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as tctime
				 , labresult as total_cholesterol
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'total cholesterol'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.tctime <= f.treatmentoffset
)
, get_tc as (
 SELECT distinct icustay_id, total_cholesterol, tctime
 from total_chole1
 WHERE last_tc =1 
)
, triglyce as (
-- 甘油三酯 
 SELECT t.icustay_id, t.triglycerides,t.trigly_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.trigly_time desc) as last_tc
 from (
  SELECT patientunitstayid as icustay_id
		     , labresultoffset as trigly_time
				 , labresult as triglycerides
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'triglycerides'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.trigly_time <= f.treatmentoffset
)
, get_triglyce as (
 SELECT distinct icustay_id, triglycerides, trigly_time
 from triglyce
 WHERE last_tc =1 
)
, hdl1 as (
SELECT t.icustay_id, t.hdl,t.hdl_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.hdl_time desc) as last_tc
 from(
  SELect patientunitstayid as icustay_id
		     , labresultoffset as hdl_time
				 , labresult as hdl
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'hdl'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.hdl_time <= f.treatmentoffset
)
, get_hdl as (
 SELECT distinct icustay_id, hdl, hdl_time
 from hdl1
 where last_tc = 1

)
, ldl1 as (
SELECT t.icustay_id, t.ldl,t.ldl_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.ldl_time desc) as last_tc
 from(
  SELect patientunitstayid as icustay_id
		     , labresultoffset as ldl_time
				 , labresult as ldl
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'ldl'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.ldl_time <= f.treatmentoffset

)
, get_ldl as (
 SELECT distinct icustay_id, ldl, ldl_time
 from ldl1
 where last_tc = 1
)
, glucose1 as (
SELECT t.icustay_id, t.glucose,t.glucose_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.glucose_time desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as glucose_time
				 , labresult as glucose
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'glucose'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.glucose_time <= f.treatmentoffset

)
, get_glucose as (
 SELECT distinct icustay_id, glucose, glucose_time
 from glucose1
 where last_tc = 1

)
, hct_base as (
  SELECT t.icustay_id, t.last_hct,t.hct_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.hct_time desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as hct_time
				 , labresult as last_hct
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'hct'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.hct_time <= f.treatmentoffset

)
, get_lasthct as (
   SELECT distinct icustay_id,  last_hct, hct_time
	 from hct_base
	 where last_tc =1
)
, minhct1 as (
SELECT t.icustay_id, t.hct, t.hct_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.hct) as mintc
  from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as hct_time
				 , labresult as hct
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'hct'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.hct_time >= f.treatmentoffset
)
, get_minhct as (
SELECT distinct icustay_id, hct as minhct
from minhct1
where mintc =1
)

, baserbc as (
SELECT t.icustay_id, t.rbc,t.rbc_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.rbc_time desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as rbc_time
				 , labresult as rbc
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'rbc'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.rbc_time <= f.treatmentoffset
)
, get_baserbc as (
SELECT distinct icustay_id, rbc as last_rbc
from baserbc
where last_tc =1
)
, minrbc1 as (
SELECT t.icustay_id, t.rbc, t.rbc_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.rbc) as mintc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as rbc_time
				 , labresult as rbc
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'rbc'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.rbc_time >= f.treatmentoffset
)
, get_minrbc as (
  SELECT distinct icustay_id, rbc as minrbc
	from minrbc1
	where mintc =1
)
, base_temp as (		
SELECT t.icustay_id, t.temp,t.rbc_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.rbc_time desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as rbc_time
				 , labresult as temp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'temperature'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.rbc_time <= f.treatmentoffset
		-- 'cpk%mb|troponin%t%|troponin%i%'
)
, get_basetemp as (
SELECT distinct icustay_id, temp as last_temp
from base_temp
where last_tc =1
)
, max_temp1 as (
SELECT t.icustay_id,  t.temp, t.rbc_time
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.temp desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset as rbc_time
				 , labresult as temp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'temperature'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.rbc_time >= f.treatmentoffset
)
, get_maxtemp as (
 SELECT distinct icustay_id, temp as max_temp
 from max_temp1
 where maxtc =1
)
, base_bnp as (
SELECT t.icustay_id, t.bnp,t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as bnp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'bnp'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_basebnp as (
 select distinct icustay_id, bnp as last_bnp
 from base_bnp
 where last_tc =1 
)
, max_bnp1 as (
  SELECT t.icustay_id,  t.bnp, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.bnp desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as bnp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'bnp'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxbnp as (
 select distinct icustay_id, bnp as maxbnp
 from max_bnp1
 where maxtc =1
)

, basetroponinI as (
SELECT t.icustay_id,  t.troponinI, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as troponinI
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'troponin%i%'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_baseTroi as (
 select distinct icustay_id, troponinI as last_troponinI
 from basetroponinI
 where last_tc =1
)
, max_troponinI1 as (
SELECT t.icustay_id,  t.troponinI, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.troponinI desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as troponinI
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'troponin%i%'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxtroi as (
 SELECT distinct icustay_id, troponinI as max_troponinI
 from max_troponinI1
 where maxtc =1
)

, basetroponinT as (
SELECT t.icustay_id,  t.troponinT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as troponinT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'troponin%t%'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_baseTrot as (
 select distinct icustay_id, troponinT as last_troponinT
 from basetroponinT
 where last_tc =1
)
, max_troponinT1 as (
SELECT t.icustay_id,  t.troponinT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.troponinT desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as troponinT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'troponin%t%'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxtroT as (
 SELECT distinct icustay_id, troponinT as max_troponinT
 from max_troponinT1
 where maxtc =1
)

,base_ckmb as (	

SELECT t.icustay_id,  t.ckmb, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as ckmb
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'cpk%mb'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
,get_baseckmb as (
 select distinct icustay_id, ckmb as last_ckmb
 from base_ckmb
 where last_tc =1
)
, max_ckmb1 as (
SELECT t.icustay_id,  t.ckmb, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.ckmb desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as ckmb
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'cpk%mb'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset

)
, get_maxckmb as (
SELECT distinct icustay_id, ckmb as max_ckmb
from max_ckmb1
where maxtc =1
)
, base_pt as (
SELECT t.icustay_id,  t.PT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'pt'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_basept as (
 select distinct icustay_id, PT as last_PT
 from base_pt
 where last_tc =1
)
, maxPT1 as (

SELECT t.icustay_id,  t.PT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.PT desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'pt'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxpt as (
SELECT distinct icustay_id, PT as max_PT
from maxPT1
where maxtc =1
)
, base_ptinr as (
SELECT t.icustay_id,  t.PTINR, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PTINR
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'pt%inr'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_baseptinr as (
SELECT distinct icustay_id, PTINR as last_ptinr
from base_ptinr
where last_tc =1
)
, maxPTINR1 as (
SELECT t.icustay_id,  t.PTINR, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.PTINR desc) as maxtc
 from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PTINR
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'pt%inr'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset

)
, get_maxPTINR as (
 SELECT distinct icustay_id, PTINR as max_ptinr
from maxPTINR1
where maxtc =1
)
, basePTT as (
SELECT t.icustay_id,  t.PTT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PTT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'ptt'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_baseptt as (
 select distinct icustay_id, PTT as last_PTT
 from basePTT
 where last_tc =1 
)
, max_ptt1 as (
  SELECT t.icustay_id,  t.PTT, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.PTT desc) as maxtc
  from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as PTT
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'ptt'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxptt as (
  select distinct icustay_id, PTT as max_PTT
	from max_ptt1
	where maxtc =1 
)
, base_wbc as (
 SELECT t.icustay_id,  t.wbc, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as wbc
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'wbc x 1000'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_basewbc as (
 SELECT distinct icustay_id, wbc as last_wbc
 from base_wbc
 where last_tc =1
)
, max_wbc1 as (
	SELECT t.icustay_id,  t.wbc, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.wbc desc) as maxtc
  from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as wbc
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'wbc x 1000'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxwbc as (
	select distinct icustay_id, wbc as max_wbc
	from max_wbc1
	where maxtc =1
)
, base_crp as (
 SELECT t.icustay_id,  t.crp, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as crp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'crp'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_basecrp as (
 select distinct icustay_id, crp as last_crp
 from base_crp
 where last_tc =1
)
, max_crp1 as (
 SELECT t.icustay_id,  t.crp, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.crp desc) as maxtc
  from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as crp
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'crp'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxcrp as (
 select distinct icustay_id, crp as max_crp
	from max_crp1
	where maxtc =1
)
, base_crphs as (
SELECT t.icustay_id,  t.crphs, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.labresultoffset desc) as last_tc
 from (
		SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as crphs
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'crp-hs'
		and labresult is not null
		)t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset <= f.treatmentoffset
)
, get_basecrphs as (
 SELECT distinct icustay_id, crphs as last_crphs
  from base_crphs
	where last_tc =1
)
, max_crphs1 as (
 SELECT t.icustay_id,  t.crphs, t.labresultoffset
      , "row_number"() over(PARTITION by t.icustay_id ORDER BY t.crphs desc) as maxtc
  from (
  SELect patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult as crphs
		from  lab
		WHERE patientunitstayid in(
		SELECT ICUSTAY_ID from IABP_detail
		)
		and lower(labname) like 'crp-hs'
		and labresult is not null
	) t
	 inner join  IABP_plt f
	 on t.icustay_id = f.icustay_id 
	 and t.labresultoffset >= f.treatmentoffset
)
, get_maxcrphs as (
 select distinct icustay_id, crphs as max_crphs
 from max_crphs1
 where maxtc =1
)
, major_bleed1 as (
  SELECT distinct patientunitstayid as icustay_id
	from diagnosis d
	inner join IABP_plt  i
	on d.patientunitstayid = i.icustay_id
	where lower(diagnosisstring) similar to '%hemorrhagic stroke%'
  and d.diagnosisoffset >= i.treatmentoffset 
)

, basic1 as (
SELECT distinct p.icustay_id
     , p.age
		 , p.gender
		 , p.weight
		 , p.height
		 , p.icu_los_hours
		 , p.icutype
		 , p.unit_mort
		 , p.hosp_mort
		 , p.hosp_los
     , p.treatmentoffset
		 , p.last_plt
		 , p.min_plt
		 , p.dpc as plt_dpc
		 , h.last_hgb
		 , h.min_hgb
		 , h.hgb_drop
		 , ch.last_crphs
		 , mh.max_crphs
		 , bcr.last_crp
		 , mcr.max_crp
		 , bw.last_wbc
		 , mw.max_wbc
		 , bptt.last_PTT
		 , mptt.max_PTT
		 , binr.last_ptinr
		 , minr.max_ptinr
		 , bpt.last_PT
		 , mpt.max_PT
		 , bck.last_ckmb
		 , mck.max_ckmb
		 , btt.last_troponinT
		 , mtt.max_troponinT
		 , bti.last_troponinI
		 , mti.max_troponinI
		 , bbn.last_bnp
		 , mbn.maxbnp
		 , btp.last_temp
		 , mtp.max_temp
		 , brbc.last_rbc
		 , mrbc.minrbc
		 , lhc.last_hct 
		 , mhc.minhct
		 , glu.glucose as last_glu
		 , hd.hdl 
     , ld.ldl
		 , tri.triglycerides
     , sp.sbp 
--      , dp.last_dbp
     , tc.total_cholesterol  

		 , case when h.hgb_drop <=0.0 then null
		       when (h.hgb_drop >0.0 and h.hgb_drop <3.0) then 1 -- minimal 
					 when (h.hgb_drop >3.0 and h.hgb_drop <=5.0) then 2 -- minor 
					 when (h.hgb_drop >5.0) then 3 -- major
					 when p.icustay_id in (SELECT distinct icustay_id from major_bleed1) then 3
					 else null
			 end as TIMIbleed
		 , cc.last_cr
		 , cc.max_cr
		 , cc.cr_rise
		 , case 
		       when cc.cr_rise < 0.5 then 0
					 when (cc.cr_rise >= 0.5 ) then 1
					 else null
			 end as ARF
		 , case when p.icustay_id in (SELECT distinct icustay_id from transfusion_rbc) then 1
		        else 0
				end as rbc_trans
		 , tr.rbc_time
		 , case when p.icustay_id in (SELECT distinct icustay_id from plt_con) then 1
		        else 0
				end as plt_trans
		 , tp.plt_time
		 , case when p.icustay_id in (SELECT distinct icustay_id from vent1)  then 1
					else 0
						end as vent
		 , tv.vent_time
		 , case when p.icustay_id in (SELECT distinct icustay_id from diaysis1)  then 1 
		      else 0
						end as renal_diaysis
		 , td.diaysis_time
 from IABP_plt p
 left join hgb_cal1 h
 on p.icustay_id = h.icustay_id
 and p.treatmentoffset = h.treatmentoffset
 left join cr_cal1 cc
  on p.icustay_id = cc.icustay_id
 and p.treatmentoffset = cc.treatmentoffset
 left join time_rbc tr
  on p.icustay_id = tr.icustay_id
left join time_pltcon tp
 on p.icustay_id = tp.icustay_id
left join time_vent tv
 on p.icustay_id = tv.icustay_id
 left join time_diaysis td
  on p.icustay_id = td.icustay_id
left join get_basecrphs ch
  on p.icustay_id = ch.icustay_id
left join get_maxcrphs mh
  on p.icustay_id = mh.icustay_id
left join get_basecrp  bcr
  on p.icustay_id = bcr.icustay_id
left join get_maxcrp mcr
  on p.icustay_id = mcr.icustay_id
left join  get_basewbc bw
  on p.icustay_id = bw.icustay_id
left join get_maxwbc mw
  on p.icustay_id = mw.icustay_id
left join get_baseptt bptt
  on p.icustay_id = bptt.icustay_id
left join get_maxptt mptt
  on p.icustay_id = mptt.icustay_id
left join get_baseptinr binr
  on p.icustay_id = binr.icustay_id
left join get_maxPTINR minr
  on p.icustay_id = minr.icustay_id
left join get_basept bpt
  on p.icustay_id = bpt.icustay_id
left join get_maxpt mpt
  on p.icustay_id = mpt.icustay_id
left join get_baseckmb bck
  on p.icustay_id = bck.icustay_id
left join get_maxckmb mck
  on p.icustay_id = mck.icustay_id
left join get_baseTrot  btt
  on p.icustay_id = btt.icustay_id
left join  get_maxtroT mtt
  on p.icustay_id = mtt.icustay_id
left join  get_baseTroi  bti
  on p.icustay_id = bti.icustay_id
left join get_maxtroi mti
  on p.icustay_id = mti.icustay_id
left join get_basebnp bbn
  on p.icustay_id = bbn.icustay_id
left join get_maxbnp mbn
  on p.icustay_id = mbn.icustay_id
left join get_basetemp btp
  on p.icustay_id = btp.icustay_id
left join get_maxtemp mtp
  on p.icustay_id = mtp.icustay_id
left join  get_baserbc brbc
  on p.icustay_id = brbc.icustay_id
left join get_minrbc mrbc
   on p.icustay_id = mrbc.icustay_id
left join  get_lasthct  lhc
  on p.icustay_id = lhc.icustay_id
left join get_minhct mhc
  on p.icustay_id = mhc.icustay_id
left join  get_glucose glu
  on p.icustay_id = glu.icustay_id
left join  get_hdl hd
  on p.icustay_id = hd.icustay_id
left join get_ldl ld
  on p.icustay_id = ld.icustay_id
left join  get_sbp sp
  on p.icustay_id = sp.icustay_id
-- left join get_dbp dp 
--   on p.icustay_id = sp.icustay_id
left join get_tc tc
  on p.icustay_id = tc.icustay_id
left join get_triglyce tri
  on p.icustay_id = tri.icustay_id

 )
 
 SELECT DISTINCT  icustay_id
     ,  age
		 ,  gender
		 ,  weight
		 ,  height
		 , icu_los_hours
		 , icutype
		 ,  unit_mort
		 , hosp_mort
		 , hosp_los
     , treatmentoffset
		 , last_plt
		 , min_plt
		 , plt_dpc
		 , last_hgb
		 ,  min_hgb
		 ,  hgb_drop
		 , last_crphs
		 , max_crphs
		 , last_crp
		 , max_crp
		 , last_wbc
		 , max_wbc
		 ,  last_PTT
		 ,  max_PTT
		 , last_ptinr
		 ,  max_ptinr
		 , last_PT
		 , max_PT
		 ,  last_ckmb
		 ,  max_ckmb
		 ,  last_troponinT
		 ,  max_troponinT
		 ,  last_troponinI
		 ,  max_troponinI
		 ,  last_bnp
		 ,  maxbnp
		 ,  last_temp
		 ,  max_temp
		 ,  last_rbc
		 ,  minrbc
		 , last_hct 
		 ,  minhct
		 ,  last_glu
		 , hdl 
     , ldl
		 , triglycerides
     , sbp 
--      , last_dbp
     , total_cholesterol  
		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , rbc_time
		 ,  plt_trans
		 , plt_time
		 , vent
		 , vent_time
		 ,  renal_diaysis
		 , diaysis_time
     , case when rbc_trans = 1 or plt_trans = 1 then 1
		        when rbc_trans = 0 and  plt_trans = 0 then 0
				else null
			end as transfusion_ind
		 , case when rbc_trans = 1 and  plt_trans = 1 then LEAST(rbc_time,plt_time)
		        when rbc_trans = 1 and  plt_trans = 0 then rbc_time
						when rbc_trans = 0 and  plt_trans = 1 then plt_time
				else null
			end as transfusion_time
 from basic1
 group by icustay_id
     ,  age
		 ,  gender
		 ,  weight
		 ,  height
		 , icu_los_hours
		  , icutype
		 ,  unit_mort
		 , hosp_mort
		 , hosp_los
     , treatmentoffset
		 , last_plt
		 , min_plt
		 , plt_dpc
		 , last_hgb
		 ,  min_hgb
		 ,  hgb_drop
		 , last_crphs
		 , max_crphs
		 , last_crp
		 , max_crp
		 , last_wbc
		 , max_wbc
		 ,  last_PTT
		 ,  max_PTT
		 , last_ptinr
		 ,  max_ptinr
		 , last_PT
		 , max_PT
		 ,  last_ckmb
		 ,  max_ckmb
		 ,  last_troponinT
		 ,  max_troponinT
		 ,  last_troponinI
		 ,  max_troponinI
		 ,  last_bnp
		 ,  maxbnp
		 ,  last_temp
		 ,  max_temp
		 ,  last_rbc
		 ,  minrbc
		 , last_hct 
		 ,  minhct
		 ,  last_glu
		 , hdl 
     , ldl
		 , triglycerides
     , sbp 
--      , last_dbp
     , total_cholesterol  
		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , rbc_time
		 ,  plt_trans
		 , plt_time
		 , vent
		 , vent_time
		 ,  renal_diaysis
		 , diaysis_time ,transfusion_ind , transfusion_time
)

-- 加入dbp（ data from vitalperiodic and vitalaperiodic table）
DROP MATERIALIZED VIEW IF EXISTS IABP_basic2 CASCADE;
CREATE MATERIALIZED VIEW IABP_basic2 as (
with dbp_data as(
SELECT distinct icustay_id, observationoffset, dbp 
from (
	select patientunitstayid as icustay_id
	      , observationoffset
				, systemicdiastolic as dbp
	from vitalperiodic vp
	where patientunitstayid in (
	 SELECT icustay_id from IABP_plt
	)
	union (
	select patientunitstayid as icustay_id
	      , observationoffset
				, noninvasivediastolic as dbp
	from vitalaperiodic vap
	where patientunitstayid in (
	 SELECT icustay_id from IABP_plt
	)
	)
	)t
	where dbp is not null 
)
, dbp1 as(
 select s.icustay_id, s.dbp
      , s.observationoffset
			, "row_number"() over(PARTITION by s.icustay_id ORDER BY s.observationoffset desc) as last_dbp
 from  dbp_data s
	inner join  IABP_plt f
	 on s.icustay_id = f.icustay_id 
	 and s.observationoffset <= f.treatmentoffset
)
, get_dbp as (
 SELECT icustay_id
	     , dbp
-- 			 , observationoffset as dbptime
 from dbp1
 where last_dbp = 1
 group by icustay_id, dbp
)
SELECT b.icustay_id
     ,  age
		 ,  gender
		 ,  weight
		 ,  height
		 , icu_los_hours
		 , icutype
		 ,  unit_mort
		 , hosp_mort
		 , hosp_los
     , treatmentoffset
		 , last_plt
		 , min_plt
		 , plt_dpc
		 , last_hgb
		 ,  min_hgb
		 ,  hgb_drop
		 , last_crphs
		 , max_crphs
		 , last_crp
		 , max_crp
		 , last_wbc
		 , max_wbc
		 ,  last_PTT
		 ,  max_PTT
		 , last_ptinr
		 ,  max_ptinr
		 , last_PT
		 , max_PT
		 ,  last_ckmb
		 ,  max_ckmb
		 ,  last_troponinT
		 ,  max_troponinT
		 ,  last_troponinI
		 ,  max_troponinI
		 ,  last_bnp
		 ,  maxbnp
		 ,  last_temp
		 ,  max_temp
		 ,  last_rbc
		 ,  minrbc
		 , last_hct 
		 ,  minhct
		 ,  last_glu
		 , hdl 
     , ldl
		 , triglycerides
     , sbp 
     , d.dbp as last_dbp
     , total_cholesterol  
		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , rbc_time
		 ,  plt_trans
		 , plt_time
		 , vent
		 , vent_time
		 ,  renal_diaysis
		 , diaysis_time ,transfusion_ind , transfusion_time
from iabp_basic1 b
left join  get_dbp d
on b.icustay_id = d.icustay_id
)



-- SELECT distinct labname from lab
-- 		where lower(labname) similar to   'hct|rbc|bnp|pt|ptt|crp|crp-hs|hdl|ldl|pt - inr|total cholesterol|triglycerides|wbc x 1000'


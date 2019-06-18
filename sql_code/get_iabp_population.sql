--  本sql 获得进行IABP的患者， 用肝素情况， 是否有手术
DROP MATERIALIZED VIEW IF EXISTS IABP_detail CASCADE;
CREATE MATERIALIZED VIEW IABP_detail as (
with firstIcu as (
     SELECT uniquepid, patienthealthsystemstayid as hadmid, patientunitstayid as ICUSTAY_ID, apacheadmissiondx  
		       , case when age like '%>%' then '91.4'
					        else age 
							end as age
					, unittype as icutype
					 , gender, apache_iv,  admissionweight, admissionheight, dischargeweight
					 , icu_los_hours
					 , unit_mort
					 , hosp_mort
					 ,hospitaldischargeoffset
					 ,unitdischargeoffset
					 , round((hospitaldischargeoffset::numeric/60::numeric),2) as hosp_dischargeoffsetH
					 , round((unitdischargeoffset::numeric/60::numeric),2) as unit_dischargeoffsetH 
           , row_number() OVER (PARTITION BY uniquepid, patienthealthsystemstayid ORDER BY unitvisitnumber) AS first_icu
     FROM eicu.icustay_detail1
)
-- , getFirstICU as(
--      SELECT  uniquepid
--            , hadmid
--            , ICUSTAY_ID
--            , apacheadmissiondx  
--            , age, gender, apache_iv, unittype, admissionweight, admissionheight, icu_los_hours
--       FROM  firstIcu
-- --     WHERE first_icu = 1
-- )
, getIABP as (
select patientunitstayid as icustay_id,
       treatmentid,
	   treatmentyear,
	   -- treatmenttime24, 
	   treatmentoffset,
	   round((treatmentoffset::numeric/60::numeric),2) as treatmentoffset_hour,
	   round((treatmentoffset::numeric/60/24::numeric),2) as treatmentoffset_day,
	   treatmentstring,
	   activeupondischarge as active
from treatment 
WHERE lower(treatmentstring) similar to '%intra%balloon%pump%|%intraaortic%'  -- balloon counterppulsation%
order by patientunitstayid, treatmentyear, treatmentoffset
)
, IABP_Detail as (
   SELECT fi.uniquepid
           , fi.hadmid
           , fi.ICUSTAY_ID 
           , fi.age :: NUMERIC
					 , fi.gender
					 , icutype
					 , fi.apache_iv, fi.admissionweight, fi.admissionheight,dischargeweight
					 , fi.icu_los_hours
					 , gi.treatmentid
					 , gi.treatmentyear 
					 , gi.treatmentoffset
	         , gi.treatmentoffset_hour
					 , treatmentstring
					 , active , fi.first_icu
					 , fi.unit_mort, fi.hosp_mort
					 , fi.hospitaldischargeoffset -- hosp_dischargeoffsetH
					 , fi.unitdischargeoffset
-- 					 , unit_dischargeoffsetH
	 from getIABP gi
	 left join firstIcu fi
    on gi.icustay_id = fi.icustay_id
	where  fi.age != ''
	  and cast(fi.age as numeric) >= 18
)
, removal_id as (
select distinct icustay_id
	      -- , treatmentstring
	from IABP_Detail pd
	WHERE lower(treatmentstring) like '%removal%'
)
, remove_iabp1 as (
-- 这里需要找出iabp全是removal的
   SELECT icustay_id, treatmentstring, count(*) as c
	 from (
  select distinct icustay_id
	      , treatmentstring
	from IABP_Detail pd
) tt  GROUP BY icustay_id, treatmentstring
)
, remove_2 as (
-- 这里需要找出既有iabp又有iabp-removal的（count= 2）
   SELECT distinct icustay_id
	 from remove_iabp1
  where c=2
)
, remove_3 as (

-- 这里需要找出只有IABP的（count= 1）
   SELECT distinct icustay_id
	 from remove_iabp1
  where c=1
	and icustay_id not in (
	-- removal_id 里面可能包含了 全是removal的
	  select distinct icustay_id  from removal_id
	)
)
, iabp_id as (
  select icustay_id from remove_3
	union(
	select icustay_id from remove_2
	)
)
, iabp_Detail2 as (
  SELECT * from IABP_Detail
	where icustay_id in (
	    select icustay_id from iabp_id
	)
)

-- , hep_med as(
-- SELECT patientunitstayid as icustay_id,
--        drugOrderOffset,
-- 	   drugstartOffset,
-- 	   drugstopoffset,
-- 	   (drugstopoffset - drugstartOffset)as  drug_duration,
-- 	   dosage 
-- -- 	   frequency
-- 
--    from medication
--  where lower(drugName) like '%heparin%'
--  and drugstartoffset >0
-- and lower(drugOrderCancelled) like 'no'
-- and patientunitstayid in (312795,313918)
-- )
-- , hep_infu as (
-- SELECT patientunitstayid as icustay_id,
--        infusionOffset as drugstartOffset,
-- 	   drugAmount as  dosage,
-- 	   volumeoffluid 
-- 
--    from infusiondrug
--  where lower(drugName) like '%heparin%'
-- 
-- ) 
-- -- 是否有做心脏造影术
-- , angiography as (
-- select patientunitstayid as icustay_id,
--        treatmentid,
-- 	   treatmentyear,
-- 	   -- treatmenttime24, 
-- 	   treatmentoffset,
-- 	   round((treatmentoffset::numeric/60::numeric),2) as treatmentoffset_hour,
-- 	   round((treatmentoffset::numeric/60/24::numeric),2) as treatmentoffset_day,
-- 	   treatmentstring,
-- 	   activeupondischarge as active
-- from treatment 
-- WHERE lower(treatmentstring) similar to '%pci%|%percutaneous coronary intervention%|%cag%|%angiography%'  -- balloon counterppulsation%
-- -- and patientunitstayid =  263651
-- )
SELECT * from iabp_Detail2
where first_icu = 1
)

SELECT distinct icustay_id from IABP_detail -- 1150 patients

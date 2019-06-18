-- 获取患者的既往病史

DROP MATERIALIZED VIEW IF EXISTS IABP_hist CASCADE;
CREATE MATERIALIZED VIEW IABP_hist as (
with hosplos_basic as (
   SELECT  patientunitstayid as icustay_id
				 , hospitaldischargeoffset, hospitaladmitoffset
	       , round((hospitaldischargeoffset - hospitaladmitoffset)/60, 5) as hosp_los
		 from patient
)
, basic as (
 SELECT b.icustay_id
     ,  b.age
		 ,  b.gender
		 , d.apache_iv
		 ,  b.weight
		 ,  b.height
		 , b.icu_los_hours
		 , b.icutype
		 ,  b.unit_mort
		 , b.hosp_mort
		 , case when b.hosp_los is null then hb.hosp_los -- icustay_id = 327227 then  27.267
		  else b.hosp_los
			end as hosp_los
     , b.treatmentoffset
		 , b.last_plt
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
     , last_dbp
     , total_cholesterol  
		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , rbc_time
		 , plt_trans
		 , plt_time
		 , vent
		 , vent_time
		 ,  renal_diaysis
		 , diaysis_time
		 , transfusion_ind 
		 , transfusion_time
	 from IABP_basic2 b
	 left join icustay_detail d
	  on b.icustay_id = d.patientunitstayid
	left join hosplos_basic hb
	  on b.icustay_id = hb.icustay_id
)
, hypertens1 as (
  SELECT distinct patientunitstayid as icustay_id 
	     , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) like '%hypertension%'
	group by patientunitstayid,pasthistorypath
)
, hypertens2 as (
  SELECT distinct patientunitstayid as icustay_id 
	from diagnosis
	where lower(diagnosisstring) similar to '%(vascular|ventricular) disorders\|hypertension%'
)
, hypertens as (
  SELECT distinct icustay_id from hypertens2
	union (
	SELECT distinct icustay_id from hypertens1
	)
)
, diabete1 as (
  SELECT distinct patientunitstayid as icustay_id 
       -- , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) like '%diabetes%'
)
, diabete2 as (
  SELECT distinct patientunitstayid as icustay_id 
    --    , diagnosisstring
	from diagnosis
	where lower(diagnosisstring) similar to '%diabetes mellitus%'
)
, diabete as (
  SELECT distinct icustay_id from diabete1
	union (
	SELECT distinct icustay_id from diabete2
	)
)
, renal_failure as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%renal failure%|%renal insufficiency%'
)
, angina as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%angina%'
)
, mi as(
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%myocardial infarction%' 
)
, pci as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%procedural coronary intervention%'
)
, cabg as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%coronary artery bypass%'
)

, chf as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%congestive heart failure%'
)
, valve as(
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%valve disease%'
)
, heart_trans as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%heart transplant%'
)
, tia as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%tia\(s%'
)
, stroke as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%stroke%'
)
, stroke_tia as (
  select distinct icustay_id from tia
	union (
	select distinct icustay_id from stroke
	)
)
, pad as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%peripheral vascular disease%'
)
, bleed as (
  SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%bleeding%' 
)
, thrombosis as (
  -- 血栓
	SELECT distinct patientunitstayid as icustay_id 
       , pasthistorypath
	from pasthistory
	where lower(pasthistorypath) similar to  '%venous thrombosis%|%pulmonary embolism%' 
)

, hypercholesterolemia as (
-- 血胆固醇过多症
  SELECT distinct patientunitstayid as icustay_id 
	from diagnosis
	where lower(diagnosisstring) similar to '%hyperlipidemia%'
)
, hit_diag as (
--  5 icustay_ids 
  SELECT  distinct patientunitstayid as icustay_id, diagnosisstring
 from diagnosis d
 where lower(d.diagnosisstring) similar to  '%(heparin induced thrombocytopenia|heparin-induced)%'
 and patientunitstayid in (
  SELECT distinct icustay_id  from iabp_basic2
 )
)

-- --------------------------------------------------------------
-- admission diagnosis
, admin_cabg1 as (
  SELECT distinct patientunitstayid as icustay_id
 from admissiondx
 where admitdxpath like '%CABG%'
)
, admin_cabg2 as (
 SELECT distinct patientunitstayid as icustay_id
 from diagnosis d
 inner join basic b 
    on d.patientunitstayid = b.icustay_id
 where lower(d.diagnosisstring) like '%cabg%'
 and  lower(d.diagnosisstring) not like '%no previous cabg%'
 and d.diagnosisoffset < b.treatmentoffset
)
, admin_cabg as (
  SELECT icustay_id from admin_cabg1
	union (
	select icustay_id from admin_cabg2
	)
)
, admin_ptca1 as (
  select distinct patientunitstayid as icustay_id
 from admissiondx
where admitdxpath like '%PTCA%'
)
, admin_ptca2 as (
  select distinct patientunitstayid as icustay_id
 from diagnosis d
 inner join basic b 
    on d.patientunitstayid = b.icustay_id
 where lower(d.diagnosisstring) like '%ptca%'
 and d.diagnosisoffset < b.treatmentoffset
)
, admin_ptca as (
 SELECT icustay_id from admin_ptca1
	union (
	select icustay_id from admin_ptca2
	)
)
, admin_mi1 as (
  SELECT distinct patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%(infarction, acute myocardial|acute mi|mi admitted)%'
) 
, admin_mi2 as (
  select distinct patientunitstayid as icustay_id
 from diagnosis d
 inner join basic b 
    on d.patientunitstayid = b.icustay_id
 where lower(d.diagnosisstring) similar to '%(acute myocardial infarction|due to myocardial infarction)%|%acute coronary syndrome\|(anterior wall|inferior wall|posterior wall)%'
 and d.diagnosisoffset < b.treatmentoffset
)
, admin_mi as (
  SELECT icustay_id from admin_mi1
	union (
	select icustay_id from admin_mi2
	)
)
, admin_angina1 as (
  SELECT distinct patientunitstayid as icustay_id
 from admissiondx
where lower(admitdxpath) similar to '%(angina|chest pain)%'
)
, admin_angina2 as (
  select distinct patientunitstayid as icustay_id
 from diagnosis d
 inner join basic b 
    on d.patientunitstayid = b.icustay_id
 where lower(d.diagnosisstring) similar to '%(unstable angina|chest pain \/ ashd\|chest pain|acute cardiac problems\|chest pain)%'
 and d.diagnosisoffset < b.treatmentoffset
)
, admin_angina as (
  SELECT icustay_id from admin_angina1
	union (
	select icustay_id from admin_angina2
	)
)
, admin_cardio_shock as (
 select distinct patientunitstayid as icustay_id
 from diagnosis d
 inner join basic b 
    on d.patientunitstayid = b.icustay_id
 where lower(d.diagnosisstring) similar to '%cardiogenic shock%'
 and d.diagnosisoffset < b.treatmentoffset
)



-- --------------------------------------------------------------
-- outcome
, tromboembolic as (
 SELECT distinct patientunitstayid as icustay_id
	from diagnosis d
	inner join IABP_basic2  i
	on d.patientunitstayid = i.icustay_id
	where lower(diagnosisstring) similar to '%(dvt|lower extremity arterial thromboembolism|pulmonary embolism|ischemic stroke)%'
  and d.diagnosisoffset >= i.treatmentoffset
)
-- --------------------------------------------------------------
-- drugs
, aspirin_usage as (
  
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%aspirin%'
	union (
	SELECT patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%aspirin%' 
	)
)
, g23I_usage as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where treatmentstring similar to '%glycoprotein IIB\/IIIA inhibitor%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%(tirofiban|abciximab|eptifibatide)%' 
	  )
 union (
 SELECT distinct patientunitstayid as icustay_id
	from infusiondrug
	where lower(drugname) similar to '%(tirofiban|abciximab|eptifibatide)%' 
 )
)
, aggr_inhib_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%aggregation inhibitors%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%clopidogrel%' 
	  )
)
, LMWH_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%low molecular weight heparin%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%enoxaparin%' 
	  )
)
, fondapar_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%factor xa inhibitor%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%fondaparinux%' 
	  )
)
, throm_inhib_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%thrombin inhibitor%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%(argatroban|bivalirudin|angiomax)%' 
	  )
 union (
 SELECT distinct patientunitstayid as icustay_id
	from infusiondrug
	where lower(drugname) similar to '%(argatroban|bivalirudin|angiomax)%' 
 )
)
, warfar_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%coumadin%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%(coumadin|warfarin)%' 
	  )
)
, thrombo_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%thrombolytic%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from medication
	where lower(drugname) similar to '%(tenecteplase)%' 
	  )
)
, heparin_use as (
  SELECT distinct patientunitstayid as icustay_id
	from treatment
	where lower(treatmentstring) similar to '%conventional heparin therapy%'
	union (
	SELECT distinct patientunitstayid as icustay_id
	from infusiondrug
	where lower(drugname) similar to '%heparin%' 
	  )
)


SELECT  * 
       , case when plt_dpc<0.25 then 1
			        when (plt_dpc >= 0.25 and  plt_dpc< 0.5) then 2
							when (plt_dpc >= 0.5 and  plt_dpc< 0.75) then 3
							when (plt_dpc >= 0.75) then 4
			        else 0
			   end as group_plt
			 , case when plt_dpc< 0.5 then 1
							when (plt_dpc >= 0.5) then 2
			        else 0
			   end as group2_plt
			 , case when min_plt > 150 then 1
			        when (min_plt > 100 and  min_plt<= 150) then 2
							when (min_plt > 50 and  min_plt<= 100) then 3
							when (min_plt <= 50) then 4
			        else 0
			   end as nadir
			 , case when height is not null and weight is not null then round((weight/((height/100)^2)),2)
			        else null
			   end as BMI
       , case when icustay_id in ( SELECT icustay_id from hypertens) then 1
			        else 0
					end as prior_hypertension
			 , case when icustay_id in ( SELECT icustay_id from diabete) then 1
			        else 0
					end as prior_diabete 
			, case when icustay_id in ( SELECT icustay_id from renal_failure) then 1
			        else 0
					end as prior_renalFail 
			, case when icustay_id in ( SELECT icustay_id from angina) then 1
			        else 0
					end as prior_angina 
			, case when icustay_id in ( SELECT icustay_id from mi) then 1
			        else 0
					end as prior_mi 
			, case when icustay_id in ( SELECT icustay_id from pci) then 1
			        else 0
					end as prior_pci 
			, case when icustay_id in ( SELECT icustay_id from cabg) then 1
			        else 0
					end as prior_cabg 
			, case when icustay_id in ( SELECT icustay_id from chf) then 1
			        else 0
					end as prior_chf 
		  , case when icustay_id in ( SELECT icustay_id from valve) then 1
			        else 0
					end as prior_valve 
			, case when icustay_id in ( SELECT icustay_id from heart_trans) then 1
			        else 0
					end as prior_heartTrans 
-- 			, case when icustay_id in ( SELECT icustay_id from tia) then 1
-- 			        else 0
-- 					end as prior_tia
-- 			, case when icustay_id in ( SELECT icustay_id from stroke) then 1
-- 			        else 0
-- 					end as prior_stroke
			, case when icustay_id in ( SELECT icustay_id from stroke_tia) then 1
			        else 0
					end as prior_strokeTia  
			, case when icustay_id in ( SELECT icustay_id from pad) then 1
			        else 0
					end as prior_pad
			, case when icustay_id in ( SELECT icustay_id from bleed) then 1
			        else 0
					end as prior_bleed
			, case when icustay_id in ( SELECT icustay_id from thrombosis) then 1
			        else 0
					end as prior_thrombosis
			, case when icustay_id in ( SELECT icustay_id from hypercholesterolemia) then 1
			        else 0
					end as prior_hypercholest	
		  , case when icustay_id in (SELECT icustay_id from tromboembolic) then 1
			       else 0 
				 end as trombo_outcome
			 , case when icustay_id in (SELECT icustay_id from admin_cabg) then 1
			       else 0 
				 end as admind_cabg
			 , case when icustay_id in (SELECT icustay_id from admin_ptca) then 1
			       else 0 
				 end as admind_ptca
			 ,case when icustay_id in (SELECT icustay_id from admin_mi) then 1
			       else 0 
				 end as admind_mi
			 , case when icustay_id in (SELECT icustay_id from admin_angina) then 1
			       else 0 
				 end as admind_angina
			 , case when icustay_id in (SELECT icustay_id from admin_cardio_shock) then 1
			       else 0 
				 end as admind_cardshock		
			 , case when icustay_id in (SELECT icustay_id from aspirin_usage) then 1
			       else 0 
				 end as aspirin_use
			,  case when icustay_id in (SELECT icustay_id from g23I_usage) then 1
			       else 0 
				 end as g23I_use
		  ,  case when icustay_id in (SELECT icustay_id from aggr_inhib_use) then 1
			       else 0 
				 end as aggrInhib_use
			 , case when icustay_id in (SELECT icustay_id from LMWH_use) then 1
			       else 0 
				 end as LMWH_use
			 , case when icustay_id in (SELECT icustay_id from fondapar_use) then 1
			       else 0 
				 end as fondapar_use
			 , case when icustay_id in (SELECT icustay_id from throm_inhib_use) then 1
			       else 0 
				 end as thromInhib_use
			 , case when icustay_id in (SELECT icustay_id from warfar_use) then 1
			       else 0 
				 end as warfar_use
			 , case when icustay_id in (SELECT icustay_id from thrombo_use) then 1
			       else 0 
				 end as thrombo_use		
			, case when icustay_id in (SELECT icustay_id from heparin_use) then 1
			       else 0 
				 end as heparin_use	
			
from basic
where icustay_id not in (
 SELECT icustay_id from hit_diag
)
)



DROP MATERIALIZED VIEW IF EXISTS IABP_comdata CASCADE;
CREATE MATERIALIZED VIEW IABP_comdata as (
with witness_bleeding as (
  SELECT distinct 
          d.patientunitstayid as icustay_id --  , diagnosisoffset  
from diagnosis d
inner join IABP_hist h
on d.patientunitstayid = h.icustay_id
where lower(diagnosisstring) similar to '%(hemorrhage|gi bleeding|hypovolemic shock\|hemorrhagic|acute blood loss anemia|hematuria|ongoing hemorr)%'
and diagnosisoffset >= h.treatmentoffset
)
, hemorrhagic_stroke as (
 SELECT distinct 
          d.patientunitstayid as icustay_id --  , diagnosisoffset  
from diagnosis d
inner join IABP_hist h
on d.patientunitstayid = h.icustay_id
where lower(diagnosisstring) similar to '%hemorrhagic stroke%'
and diagnosisoffset >= h.treatmentoffset
)
, hgb as (
-- 血红蛋白  hgb
		SELect l.patientunitstayid as icustay_id
		     , labresultoffset
				 , labresult
				 , labmeasurenamesystem
				 , labmeasurenameinterface
				 , h.treatmentoffset
		from  lab l
		inner join IABP_hist h
on l.patientunitstayid = h.icustay_id
	where lower(l.labname) like 'hgb'
		and labresult is not null
-- 		and l.labresultoffset >= h.treatmentoffset
)
, hgb_afterIABP as (
-- 血红蛋白  hgb
		SELect l.icustay_id
		     , l.labresultoffset
				 , labresult
				 , labmeasurenamesystem
				 , labmeasurenameinterface
				 , l.treatmentoffset
		from  hgb l
		inner join IABP_hist h
on l.icustay_id = h.icustay_id
	where l.labresultoffset >= h.treatmentoffset
)
, min_hgb as (
  SELECT icustay_id
       	, labresultoffset
				, labresult ,treatmentoffset
	from 
	(SELECT  icustay_id
		     ,  labresultoffset
				 , labresult
				 ,  treatmentoffset
				 , "row_number"() over(PARTITION by icustay_id order by labresult) as min_hgb_ind
	  from hgb_afterIABP
	)t
	where min_hgb_ind = 1
)
, hgb_72h as (
  SELECT h.icustay_id
     , h.treatmentoffset
-- 		 , fi.treatmentoffset_hour
		 , h.labresultoffset
		 , h.labresult
		 , h.labmeasurenamesystem
		 , h.labmeasurenameinterface
	 from hgb h
	 inner join min_hgb pt
	 on h.icustay_id = pt.icustay_id
 where h.labresultoffset < pt.labresultoffset
 and h.labresultoffset >= (pt.labresultoffset - 4320) 
)
, max_hgb72 as(
 SELECT icustay_id
       	, labresultoffset
				, labresult as max_hgb72
				, treatmentoffset
	from 
	(SELECT  icustay_id
		     ,  labresultoffset
				 , labresult
				 ,  treatmentoffset
				 , "row_number"() over(PARTITION by icustay_id order by labresult desc) as max_hgb_ind
	  from hgb_72h
	)t
	where max_hgb_ind = 1
)
, max_hgb as (
SELECT icustay_id
       	, labresultoffset
				, labresult as max_hgb
				, treatmentoffset
 from 
(SELECT h.icustay_id
     , h.treatmentoffset
-- 		 , fi.treatmentoffset_hour
		 , h.labresultoffset
		 , h.labresult
		 , h.labmeasurenamesystem
		 , h.labmeasurenameinterface
		 , "row_number"() over(PARTITION by h.icustay_id order by h.labresult desc) as max_hgb_ind
	 from hgb h
	 inner join min_hgb pt
	 on h.icustay_id = pt.icustay_id
 where h.labresultoffset < pt.labresultoffset
--  and h.labresultoffset >= (pt.labresultoffset - 4320) 
) t1
where max_hgb_ind =1 
)
-- SELECT distinct icustay_id from max_hgb; -- 710
-- SELECT distinct icustay_id from min_hgb; -- 710
-- SELECT distinct icustay_id from max_hgb72; -- 702
, hgb_delta as(
  SELECT m.icustay_id
	     , m.labresult as min_hgb
			 , a7.max_hgb72 
			 , a.max_hgb 
			 , (a.max_hgb - m.labresult) as hgb_delta
			 , (a7.max_hgb72 - m.labresult) as hgb_delta72
			 , m.treatmentoffset
-- 			 , m.labmeasurenamesystem
			 , m.labresultoffset as min_hgb_time
			 , a7.labresultoffset as max_hgb71_time
			 , a.labresultoffset as max_hgb_time
	from min_hgb m
	left join  max_hgb72 a7
	on m.icustay_id = a7.icustay_id
	left join max_hgb  a
	on m.icustay_id = a.icustay_id
)
, basic_p as (
select  h.icustay_id
     ,  h.age
		 ,  h.gender
		 , h.apache_iv
-- 		 ,  weight
-- 		 ,  height
		 ,  h.icu_los_hours
		 ,  h.icutype
		 ,  h.unit_mort
		 , h.hosp_mort
		 , h.hosp_los
		 , case when lower(i.ethnicity) like '%other%' or i.ethnicity = '' then 'other'
		    else i.ethnicity
				end as ethnicity
		 , i.hospitaladmitoffset as delta_admi
     , h.treatmentoffset
		 , last_plt
		 , min_plt
		 , plt_dpc
		 , last_hgb
		 ,  h.min_hgb as ori_minhgb
		 ,  h.hgb_drop as ori_hgbdrop
-- --------------------------------------------------
     , d.min_hgb
			 , d.max_hgb72 
			 , d.max_hgb 
			 , d.hgb_delta
			 , d.hgb_delta72
       , case when h.icustay_id in (SELECT icustay_id from witness_bleeding) then 1 else 0 end as witness_bleed
			 , case when h.icustay_id in (SELECT icustay_id from hemorrhagic_stroke) then 1 else 0 end as hemo_stroke 
		 , last_wbc
		 , max_wbc
		 ,  last_PTT
		 ,  max_PTT
		 , last_ptinr
		 ,  max_ptinr
		 , last_PT
		 , max_PT

		 ,  last_rbc
		 ,  minrbc
		 , last_hct 
		 ,  minhct
		 ,  last_glu
     , sbp 
     , last_dbp
		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , plt_trans
		 , vent
		 ,  renal_diaysis
		 , transfusion_ind 
-- 		 , group_plt 
		 , group2_plt as group_plt
		 , nadir
		 , BMI
		 , prior_hypertension
		 , prior_diabete 
		 , prior_renalFail
	   , prior_angina 
		 , prior_mi 
		 , prior_pci 
		 , prior_cabg 
		 , prior_chf 
		 , prior_valve  
		 , prior_heartTrans 
		 , prior_strokeTia 
		 , prior_pad 
		 , prior_bleed 
		 , prior_thrombosis 
		 , prior_hypercholest 
		 , trombo_outcome 
		 , admind_cabg 
		 , admind_ptca 
		 , admind_mi 
		 , admind_angina 
		 , admind_cardshock	
		 , aspirin_use 
		 , g23I_use 
		 , aggrInhib_use 
		 , LMWH_use 
		 , fondapar_use 
		 , thromInhib_use 
		 , warfar_use
		 , thrombo_use		 
		 , heparin_use	
from IABP_hist h
left join icustay_detail i
on h.icustay_id = i.patientunitstayid
left join hgb_delta d
on h.icustay_id = d.icustay_id

)
SELECT  icustay_id
     ,  age
		 , round(age/10, 3) as  age_10
		 ,  gender
		 ,  apache_iv
		 , round(apache_iv/10, 3) as  apache_iv_10
-- 		 ,  weight
-- 		 ,  height
		 ,  icu_los_hours
		 ,  icutype
		 ,  unit_mort
		 ,  hosp_mort
		 ,  hosp_los
		 ,  ethnicity
		 , case when lower(ethnicity) = 'caucasian' then 1
		        else 0 
				end as ethnicity_group
		 ,  delta_admi
     , treatmentoffset
		 , last_plt
		 , round(last_plt/10, 3) as  last_plt_10
		 , min_plt
		 , round(min_plt/10, 3) as  min_plt_10
		 , plt_dpc
		 , (plt_dpc*10) as plt_dpc10
		 , last_hgb
		 , ori_minhgb
		 , ori_hgbdrop
-- --------------------------------------------------
--      ,  min_hgb
-- 			 ,  max_hgb72 
-- 			 ,  max_hgb 
-- 			 ,  hgb_delta
			 ,  hgb_delta72
       ,  case when witness_bleed =1 and hgb_delta72 >3 then 1
			         when hemo_stroke =1 then 1
							 when hgb_delta72 > 5 then 1
							 when rbc_trans =1 then 1
						 else 0
					 end as major_bleed
-- 			 , hemo_stroke 
		 , last_wbc
		 , max_wbc
-- 		 ,  last_PTT
-- 		 ,  max_PTT
		 , last_ptinr
		 ,  max_ptinr
		 , last_PT
		 , max_PT
-- 		 ,  last_rbc
-- 		 ,  minrbc
-- 		 , last_hct 
-- 		 ,  minhct
      ,last_glu
		 ,  round(last_glu/18,3) as last_glu_18
     , sbp 
     , last_dbp
		 , round((sbp/3)::NUMERIC + (last_dbp * (2/3))::NUMERIC,4) as MAP_bp
-- 		 , TIMIbleed
		 , last_cr
		 , max_cr
		 , cr_rise
		 , ARF
		 , rbc_trans
		 , plt_trans
		 , vent
		 ,  renal_diaysis
		 , transfusion_ind 
		 , group_plt 
		 , nadir -- 
		 , BMI
		 , prior_hypertension
		 , prior_diabete 
		 , prior_renalFail
	   , prior_angina 
		 , prior_mi 
		 , prior_pci 
		 , prior_cabg 
		 , prior_chf 
		 , prior_valve  
		 , prior_heartTrans 
		 , prior_strokeTia 
		 , prior_pad 
		 , prior_bleed 
		 , prior_thrombosis 
		 , prior_hypercholest 
		 , trombo_outcome 
		 , admind_cabg 
		 , admind_ptca 
		 , admind_mi 
		 , admind_angina 
		 , admind_cardshock	
		 , aspirin_use 
		 , g23I_use 
		 , aggrInhib_use 
		 , LMWH_use 
		 , fondapar_use 
		 , thromInhib_use 
		 , warfar_use
		 , thrombo_use		 
		 , heparin_use	
 from basic_p
)


SELECT * from IABP_comdata;
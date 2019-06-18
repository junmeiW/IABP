-- ------------------------------------------------------------------
-- Title: ICU stay detail
-- Description: Each row represents a single ICU stay. Patient demographics
--        are summarised for each stay.
-- ------------------------------------------------------------------

-- (Optional) Define which schema to work on
-- SET search_path TO eicu_crd;

DROP MATERIALIZED VIEW IF EXISTS icustay_detail1 CASCADE;
CREATE MATERIALIZED VIEW icustay_detail1 as (

SELECT pt.uniquepid, pt.patienthealthsystemstayid, pt.patientunitstayid, pt.unitvisitnumber,  
       pt.hospitalid, h.region, pt.unittype,  pt.apacheadmissiondx,
       pt.hospitaladmitoffset, pt.hospitaldischargeoffset, 
       0 AS unitadmitoffset, pt.unitdischargeoffset,
       ap.apachescore AS apache_iv,    
       pt.hospitaladmityear, pt.unitadmityear,
       pt.age, 
       CASE WHEN lower(pt.hospitaldischargestatus) like '%alive%' THEN 0
            WHEN lower(pt.hospitaldischargestatus) like '%expired%' THEN 1 
            ELSE NULL END AS hosp_mort,
			case when lower(pt.unitdischargestatus) like '%alive%' THEN 0
						WHEN lower(pt.hospitaldischargestatus) like '%expired%' THEN 1 
            ELSE NULL END AS unit_mort,
			
       CASE WHEN lower(pt.gender) like '%female%' THEN 0
            WHEN lower(pt.gender) like '%male%' THEN 1
            ELSE NULL END AS gender, 
       pt.ethnicity, pt.admissionheight, pt.admissionweight, pt.dischargeweight,
       ROUND(pt.unitdischargeoffset/60) AS icu_los_hours
-- 			, DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) AS hospstay_seq
-- 			, CASE
--     WHEN DENSE_RANK() OVER (PARTITION BY adm.subject_id ORDER BY adm.admittime) = 1 THEN 'Y'
--     ELSE 'N' END AS first_hosp_stay
--       , DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq
-- 
-- -- first ICU stay *for the current hospitalization*
-- 			, CASE
-- 					WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1 THEN 'Y'
-- 					ELSE 'N' END AS first_icu_stay

FROM patient pt
LEFT JOIN hospital h
    ON pt.hospitalid = h.hospitalid
LEFT JOIN apachepatientresult ap
    ON pt.patientunitstayid = ap.patientunitstayid
    AND ap.apacheversion = 'IV'
ORDER BY pt.uniquepid, pt.unitvisitnumber, pt.age
);

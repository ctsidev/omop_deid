# DATABRICKS OMOP_DEID CODE SNIPPETS
## Code snippets for UCDDP Databricks SQL to produce EHR like tables using OMOP_DEID 

## Patients => Person
```
drop table if exists xdr_pat;
create table xdr_pat as
select p.person_id
  ,gen.concept_name gender
  ,year_of_birth 
  ,race.concept_name race
  ,eth.concept_name ethnicity
  ,case when death_date is null then 'Not Known Deceased' else 'Known Deceased' end as vital_status
  ,death_date
from xdr_coh coh
join omop_deid.person p on coh.person_id = p.person_id
left join omop_deid.concept gen on gen.concept_id = p.gender_concept_id
left join omop_deid.concept race on race.concept_id = p.race_concept_id
left join omop_deid.concept eth on eth.concept_id = p.ethnicity_concept_id
left join omop_deid.death d on coh.person_id = d.person_id
;
```

## Encounters => Visit_Occurrence
```
drop table if exists xdr_enc;
create table xdr_enc as
select coh.person_id
  ,vo.visit_occurrence_id
  ,et.concept_name encounter_type
  ,visit_start_date
  ,visit_end_date
  ,adm.concept_name admitting_source
  ,dis.concept_name discharge_to
  ,care_site_name
  ,pos.concept_name place_of_service
  ,location_source_value location
from xdr_coh coh
join omop_deid.visit_occurrence vo on coh.person_id = vo.person_id
join omop_deid.visit_detail vd on vd.visit_occurrence_id = vo.visit_occurrence_id
left join omop_deid.concept et on et.concept_id = vo.visit_concept_id
left join omop_deid.care_site cs on cs.care_site_id = vo.care_site_id
left join omop_deid.location loc on loc.location_id = cs.location_id
left join omop_deid.concept pos on pos.concept_id = cs.place_of_service_concept_id
left join omop_deid.concept adm on adm.concept_id = vo.admitting_source_concept_id
left join omop_deid.concept dis on dis.concept_id = vo.discharge_to_concept_id
;
```

## Diagnoses => Condition_Occurrence
```
drop table if exists xdr_dx;
create table xdr_dx as
select distinct co.person_id
  ,co.visit_occurrence_id
  ,co.condition_occurrence_id
  ,co.condition_start_date
  ,cs_con.vocabulary_id as diagnosis_type
  ,cs_con.concept_code as diagnosis_code
  ,cs_con.concept_name diagnosis_description
from omop_deid.condition_occurrence co
join xdr_coh coh on co.person_id = coh.person_id
join omop_deid.concept cs_con on co.condition_source_concept_id = cs_con.concept_id 
    and domain_id = 'Condition' and vocabulary_id in ('ICD10CM','ICD9CM')
;
```

## Procedures => Procedure_Occurrence
```
drop table if exists xdr_proc;
create table xdr_proc as
select distinct po.person_id
  ,po.procedure_occurrence_id
  ,po.visit_occurrence_id
  ,po.procedure_date
  ,p_con.concept_name procedure_description
  ,p_con.vocabulary_id procedure_type
  ,p_con.concept_code procedure_code
from omop_deid.procedure_occurrence po
join xdr_coh coh on po.person_id = coh.person_id
left join omop_deid.concept p_con on p_con.concept_id = po.procedure_concept_id and domain_id = 'Procedure'
   and vocabulary_id in ('CPT4','ICD9CM','ICD10CM','ICD9Proc','ICD10PCS')
;
```

## Medications => Drug_Exposure
```
drop table if exists xdr_med;
create table xdr_med as
select distinct de.person_id
  ,visit_occurrence_id
  ,drug_exposure_id
  ,dcon.concept_name medication_name
  ,dcon.vocabulary_id source
  ,drug_exposure_start_date start_date
  ,drug_exposure_end_date end_date
  ,refills
  ,quantity
  ,sig
from omop_deid.drug_exposure de
join xdr_coh coh on de.person_id = coh.person_id
left join omop_deid.concept dcon on de.drug_concept_id = dcon.concept_id
```

## Labs => Measurements
```
drop table if exists xdr_lab;
create table xdr_lab as
select distinct m.person_id
  ,visit_occurrence_id
  ,measurement_id
  ,measurement_datetime
  ,m_con.concept_name lab_name
  ,m_con.vocabulary_id procedure_code_type
  ,m_con.concept_code procedure_code
  ,value_as_number
  ,range_low
  ,range_high
from xdr_coh coh
join omop_deid.measurement m on coh.person_id = m.person_id
left join omop_deid.concept m_con on m.measurement_concept_id = m_con.concept_id
;
```

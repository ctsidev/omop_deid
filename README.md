# DATABRICKS OMOP_DEID CODE SNIPPETS
## Code snippets for UCDDP Databricks SQL to produce EHR like tables using OMOP_DEID 

## Patient => Person
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
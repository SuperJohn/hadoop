set default_parallel 50;

/*
GENERATE TOTAL CONTROL REQUEST COUNT
 */

optsoa_tracking = load '/user/hive/warehouse/rsyslog_optsoa_tracking/d=$d/b=*/dummy_server/*.gz' using PigStorage('\u0001') as (
tracking_id: chararray,
algoid: chararray,
control_test: chararray,
udid: chararray,
respond_time: chararray,
respond_status: chararray,
call_time: chararray,
opt_soa_type: chararray,
exp_label: chararray,
comment: chararray,
soa_server: chararray,
etl_day: chararray,
etl_hour: chararray,
day: chararray,
class_lable: chararray,
class_lable1: chararray,
class_lable2: chararray,
class_lable3: chararray,
source: chararray,
ad_view_type: chararray
);

/*
LOAD COLS & EXECUTE CONTROL VS TEST LOGIC
 */

optsoa_tracking_new_old = foreach optsoa_tracking generate
  tracking_id,
  control_test as bucket,
  udid,
  exp_label,
  (exp_label is null ? 'unknown' :
   (LOWER(exp_label)  matches '.*offerwall.new' ? 'new_user' :
    (LOWER(exp_label) matches '.*offerwall.old' ? 'old_user' : 'unknown')
   )
  ) as new_old_user,
  (exp_label is null or control_test is null ? 'unknown' :
   (LOWER(exp_label)  matches '.*offerwall.new' ? ( (control_test == '7' or control_test == '8' or control_test == '9') ? 'test' : 'control' ) :
    (LOWER(exp_label) matches '.*offerwall.old' ? ( (control_test == '1' or control_test == '2' or control_test == '3') ? 'test' : 'control') : 'unknown')
   )
  ) as control_test;

/*
FILTER FOR CONTROL REQUESTS ONLY
 */

control_requests = filter optsoa_tracking_new_old by control_test == 'control';

/*
GET COUNT OF CONTROL REQUESTS BY CONTROL TEST (sample results (control, 20533845) )
 */

total_requests = foreach (group control_requests by control_test) generate
  FLATTEN(group) as (control_test),
  COUNT(control_requests) as requests;

---------------------------------------------------------------------------------------------

/*
LOOK ONLY AT OFFER IDS THAT WERE EXPLICITLY TARGETED BY PERSONA , PERSONA TARGETING WAS EMPLOYED
  SAMPLE RECORD FROM targeting_offers:
  (1a38c0b8-ad0e-4087-9716-46cf832ac7f3,,,,"[""34"",""44"",""49""]")
 */

targeted_offers = load '/user/ds/sjeganathan/targeted_offers/targeting_offers_2015-09-10' using PigStorage('$') as (
  offer_id: chararray,
  gender: chararray,
  age: chararray,
  income: chararray,
  personas: chararray
);

/*
FILTER WHERE TARGETING USING PERSONA && NOT USING OTHER TARGETING
 */

persona_offers = filter targeted_offers by
  (gender is null)
  and (age is null or age == '[]')
  and (income is null or income == '[]')
  and (personas is not null and personas != '[]' and personas != '');

persona_1 = load '/user/hive/warehouse/rsyslog_OptSOAPersona/d=$d/part*' using parquet.pig.ParquetLoader();
/*
To get Parquet schema: DESCRIBE persona_1 ;
GET AD_VIEW_ID, UDID, persona_id
 */

persona_target_1 = foreach (join persona_1 by primary_offerId, persona_offers by offer_id) generate
  persona_1::ad_viewid as avi,
  persona_1::udid as udid,
  persona_1::primary_offerId as offer_id,
  (chararray)exact_match_personaid as persona_id;

/*
COUNT DISTINCT USERS - looks like this is counting all (ad_view_id, udid) pairs where the request was identified to
have explicitly used persona targeting
 */
 
persona_target_1_cnt_pre = DISTINCT(foreach persona_target_1 generate avi, udid);
persona_target_1_cnt = foreach (group persona_target_1_cnt_pre ALL) generate COUNT(persona_target_1_cnt_pre) as accepted_cnt;

persona_1_ar_ratio_overall_cross = CROSS persona_target_1_cnt, total_requests;

persona_1_ar_ratio_overall_pre = foreach persona_1_ar_ratio_overall_cross generate
  persona_target_1_cnt::accepted_cnt as accepted_requests,
  (total_requests::requests - persona_target_1_cnt::accepted_cnt) as rejected_requests,
  total_requests::requests as total_requests;

persona_1_ar_ratio_overall = foreach persona_1_ar_ratio_overall_pre generate
  accepted_requests,
  (float)accepted_requests/(float)total_requests as accepted_ratio,
  rejected_requests,
  (float)rejected_requests/(float)total_requests as rejected_ratio,
  total_requests;

-- Offer level Count
persona_target_1_offer_accpeted_pre = DISTINCT(foreach persona_target_1 generate avi, udid, offer_id);

persona_target_1_offer_accpeted = foreach (group persona_target_1_offer_accpeted_pre by (offer_id)) generate
  FLATTEN(group) as (offer_id),
  COUNT(persona_target_1_offer_accpeted_pre) as accepted_requests;

persona_target_1_offer_accpeted_w_target = foreach (join persona_target_1_offer_accpeted by offer_id, persona_offers by offer_id) generate
  persona_target_1_offer_accpeted::offer_id as offer_id,
  persona_offers::personas as personas,
  persona_target_1_offer_accpeted::accepted_requests as accepted_requests;




-- Persona Count
persona_target_1_persona_lvl_requests_pre = DISTINCT(foreach persona_target_1 generate avi, udid, persona_id);

persona_target_1_persona_lvl_requests = foreach (group persona_target_1_persona_lvl_requests_pre by persona_id) generate
  FLATTEN(group) as (persona_id),
  COUNT(persona_target_1_persona_lvl_requests_pre) as requests;





persona_1_offer_ar_ratio_cross = CROSS persona_target_1_offer_accpeted_w_target, persona_target_1_persona_lvl_requests;

persona_1_offer_ar_ratio_pre = foreach persona_1_offer_ar_ratio_cross generate
  persona_target_1_offer_accpeted_w_target::offer_id as offer_id,
  persona_target_1_offer_accpeted_w_target::personas as personas,
  persona_target_1_offer_accpeted_w_target::accepted_requests as accepted_requests,
  persona_target_1_persona_lvl_requests::persona_id as persona_id,
  persona_target_1_persona_lvl_requests::requests as requests;

persona_1_offer_ar_ratio_filtered = filter persona_1_offer_ar_ratio_pre by INDEXOF(personas, persona_id, 0) >= 0;

persona_1_offer_ar_ratio = foreach (group persona_1_offer_ar_ratio_filtered by offer_id) generate
  FLATTEN(group) as (offer_id),
  AVG(persona_1_offer_ar_ratio_filtered.accepted_requests) as accepted_requests,
  SUM(persona_1_offer_ar_ratio_filtered.requests) as requests;



rmf /user/etl/jbattles/persona_1_ab/ar_ratio_accepted/d=$dir_name;
rmf /user/etl/jbattles/persona_1_ab/ar_ratio_total_requests/d=$dir_name;
rmf /user/etl/jbattles/persona_1_ab/ar_ratio_overall/d=$dir_name;
rmf /user/etl/jbattles/persona_1_ab/ar_ratio_offer/d=$dir_name;

store persona_target_1_cnt into '/user/etl/jbattles/persona_1_ab/ar_ratio_accepted/d=$dir_name' using PigStorage();
store total_requests into '/user/etl/jbattles/persona_1_ab/ar_ratio_total_requests/d=$dir_name' using PigStorage();
store persona_1_ar_ratio_overall into '/user/etl/jbattles/persona_1_ab/ar_ratio_overall/d=$dir_name' using PigStorage();
store persona_1_offer_ar_ratio into '/user/etl/jbattles/persona_1_ab/ar_ratio_offer/d=$dir_name' using PigStorage();



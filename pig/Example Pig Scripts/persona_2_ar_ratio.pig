set default_parallel 50;

targeted_offers = load '/user/ds/sjeganathan/targeted_offers/targeting_offers_$d' using PigStorage('$') as (
  offer_id: chararray,
  gender: chararray,
  age: chararray,
  income: chararray,
  personas: chararray
);

persona_offers = filter targeted_offers by
  (gender is null)
  and (age is null or age == '[]')
  and (income is null or income == '[]')
  and (personas is not null and personas != '' and personas != '[]');

target_2 = load '/user/hive/warehouse/OptSOATargeting2/d=$d/*/*.gz' using PigStorage('\u0001') as (
  etl_date: chararray,
  holder: chararray,
  optsoa_date: chararray,
  avi: chararray,
  udid: chararray,
  offer_id: chararray,
  device_age: chararray,
  device_gender: chararray,
  device_income: chararray,
  device_persona: chararray,
  device_hispanic: chararray,
  device_survey: chararray,
  offer_age: chararray,
  offer_gender: chararray,
  offer_income: chararray,
  offer_persona: chararray,
  offer_hispanic: chararray,
  accepted: chararray,
  pub_targeting: chararray,
  publisher_app_id: chararray,
  is_survey: chararray
);

target_2_persona = foreach (join target_2 by offer_id, persona_offers by offer_id) generate
  target_2::avi as avi,
  target_2::udid as udid,
  target_2::offer_id as offer_id,
  target_2::accepted as accepted;

ar_cnt = foreach (group target_2_persona by accepted) generate
  FLATTEN(group) as accepted,
  COUNT(target_2_persona) as requests;

SPLIT ar_cnt into accepted_cnt IF accepted == 'true', rejected_cnt IF accepted=='false';

ar_cross = CROSS accepted_cnt, rejected_cnt;

ar_ratio = foreach ar_cross generate
  accepted_cnt::requests as accepted_requests,
  (float)accepted_cnt::requests/(float)(accepted_cnt::requests + rejected_cnt::requests) as accepted_ratio,
  rejected_cnt::requests as rejected_requests,
  (float)rejected_cnt::requests/(float)(accepted_cnt::requests + rejected_cnt::requests) as rejected_ratio,
  (accepted_cnt::requests + rejected_cnt::requests) as total_requests;

offer_lvl_ar_cnt = foreach (group target_2_persona by (offer_id, accepted)) generate
  FLATTEN(group) as (offer_id, accepted),
  COUNT(target_2_persona) as requests;

SPLIT offer_lvl_ar_cnt into offer_accepted_cnt IF accepted == 'true', offer_rejected_cnt IF accepted=='false';

offer_ar_ratio_pre = foreach (join offer_accepted_cnt by offer_id full, offer_rejected_cnt by offer_id) generate
  (offer_accepted_cnt::offer_id is null ? offer_rejected_cnt::offer_id : offer_accepted_cnt::offer_id) as offer_id,
  (offer_accepted_cnt::requests is null ? 0 : offer_accepted_cnt::requests) as accepted_requests,
  (offer_rejected_cnt::requests is null ? 0 : offer_rejected_cnt::requests) as rejected_requests,
  (offer_accepted_cnt::requests is null ? 0 : offer_accepted_cnt::requests) + (offer_rejected_cnt::requests is null ? 0 : offer_rejected_cnt::requests) as total_requests;

offer_ar_ratio = foreach offer_ar_ratio_pre generate
  offer_id,
  accepted_requests,
  (float)accepted_requests/total_requests as accepted_ratio,
  rejected_requests,
  (float)rejected_requests/total_requests as rejected_ratio,
  total_requests;



rmf /user/etl/jbattles/persona_2_ab/ar_ratio_overall/d=$dir_name;
rmf /user/etl/jbattles/persona_2_ab/ar_ratio_offer/d=$dir_name;

store ar_ratio into '/user/etl/jbattles/persona_2_ab/ar_ratio_overall/d=$dir_name' using PigStorage();
store offer_ar_ratio into '/user/etl/jbattles/persona_2_ab/ar_ratio_offer/d=$dir_name' using PigStorage();



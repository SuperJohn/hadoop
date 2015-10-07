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

target_2_persona_accepted = filter target_2_persona by accepted == 'true';

views = load '/user/hive/warehouse/views/d=$d/part*' using parquet.pig.ParquetLoader();

impressions = filter views by
  source == 'offerwall'
  or source == 'tj_games';
  --or (source == 'direct_play' and path == '[featured_offer_rendered]')
  --or (source == 'featured' and path == '[featured_offer_rendered]');

impressions_proj = foreach impressions generate
  ad_view_id,
  udid,
  offer_id;

target_2_impressions = foreach (join impressions_proj by (ad_view_id, udid, offer_id),
                            target_2_persona_accepted by (avi, udid, offer_id) ) generate
  target_2_persona_accepted::offer_id as offer_id,
  target_2_persona_accepted::avi as avi,
  target_2_persona_accepted::udid as udid;

target_2_impressions_overall = foreach (group target_2_impressions ALL) generate COUNT(target_2_impressions) as impressions;
target_2_impressions_offer = foreach (group target_2_impressions by offer_id) generate
  FLATTEN(group) as (offer_id),
  COUNT(target_2_impressions) as impressions;

actions = load '/user/hive/warehouse/actions/d=$d/part*' using parquet.pig.ParquetLoader();

actions_proj = foreach actions generate
  ad_view_id,
  udid,
  offer_id,
  advertiser_amount;

target_2_actions = foreach (join actions_proj by (ad_view_id, udid, offer_id),
                    target_2_persona_accepted by (avi, udid, offer_id) ) generate
  target_2_persona_accepted::avi as avi,
  target_2_persona_accepted::udid as udid,
  target_2_persona_accepted::offer_id as offer_id,
  actions_proj::advertiser_amount as advertiser_amount;

target_2_actions_overall = foreach (group target_2_actions ALL) generate
  COUNT(target_2_actions) as conversions,
  SUM(target_2_actions.advertiser_amount) * -0.01 as spend;

target_2_actions_offer = foreach (group target_2_actions by offer_id) generate
  FLATTEN(group) as (offer_id),
  COUNT(target_2_actions) as conversions,
  SUM(target_2_actions.advertiser_amount) * -0.01 as spend;

target_2_ecpm_cvr_cross = CROSS target_2_impressions_overall, target_2_actions_overall;
target_2_ecpm_cvr_overall = foreach target_2_ecpm_cvr_cross generate
  target_2_impressions_overall::impressions as impressions,
  target_2_actions_overall::conversions as conversions,
  target_2_actions_overall::spend as spend,
  (float)target_2_actions_overall::conversions/(float)target_2_impressions_overall::impressions as cvr,
  (float)target_2_actions_overall::spend/(float)target_2_impressions_overall::impressions * 1000 as ecpm;

target_2_ecpm_cvr_offer = foreach (join target_2_impressions_offer by offer_id left, target_2_actions_offer by offer_id) generate
  target_2_impressions_offer::offer_id as offer_id,
  target_2_impressions_offer::impressions as impressions,
  (target_2_actions_offer::conversions is null ? 0 : target_2_actions_offer::conversions) as conversions,
  (target_2_actions_offer::spend is null ? 0 : target_2_actions_offer::spend) as spend,
  (float)(target_2_actions_offer::conversions is null ? 0 : target_2_actions_offer::conversions)/(float)target_2_impressions_offer::impressions as cvr,
  (target_2_actions_offer::spend is null ? 0 : target_2_actions_offer::spend)/(float)target_2_impressions_offer::impressions * 1000 as ecpm;


rmf /user/etl/jbattles/persona_2_ab/ecpm_cvr_overall/d=$dir_name;
rmf /user/etl/jbattles/persona_2_ab/ecpm_cvr_offer/d=$dir_name;

store target_2_ecpm_cvr_overall into '/user/etl/jbattles/persona_2_ab/ecpm_cvr_overall/d=$dir_name' using PigStorage();
store target_2_ecpm_cvr_offer into '/user/etl/jbattles/persona_2_ab/ecpm_cvr_offer/d=$dir_name' using PigStorage();





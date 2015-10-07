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

persona_1 = load '/user/hive/warehouse/rsyslog_OptSOAPersona/d=$d/part*' using parquet.pig.ParquetLoader();

persona_target_1 = foreach (join persona_1 by primary_offerId, persona_offers by offer_id) generate
  persona_1::ad_viewid as avi,
  persona_1::udid as udid,
  persona_1::primary_offerId as offer_id,
  (chararray)exact_match_personaid as persona_id;

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

target_1_impressions = foreach (join impressions_proj by (ad_view_id, udid, offer_id),
                                     persona_target_1 by (avi, udid, offer_id) ) generate
  persona_target_1::offer_id as offer_id,
  persona_target_1::avi as avi,
  persona_target_1::udid as udid;

target_1_impressions_overall = foreach (group target_1_impressions ALL) generate COUNT(target_1_impressions) as impressions;

target_1_impressions_offer = foreach (group target_1_impressions by offer_id) generate
  FLATTEN(group) as (offer_id),
  COUNT(target_1_impressions) as impressions;

actions = load '/user/hive/warehouse/actions/d=$d/part*' using parquet.pig.ParquetLoader();

actions_proj = foreach actions generate
  ad_view_id,
  udid,
  offer_id,
  advertiser_amount;

target_1_actions = foreach (join actions_proj by (ad_view_id, udid, offer_id),
                             persona_target_1 by (avi, udid, offer_id) ) generate
  persona_target_1::avi as avi,
  persona_target_1::udid as udid,
  persona_target_1::offer_id as offer_id,
  actions_proj::advertiser_amount as advertiser_amount;

target_1_actions_overall = foreach (group target_1_actions ALL) generate
  COUNT(target_1_actions) as conversions,
  SUM(target_1_actions.advertiser_amount) * -0.01 as spend;

target_1_actions_offer = foreach (group target_1_actions by offer_id) generate
  FLATTEN(group) as (offer_id),
  COUNT(target_1_actions) as conversions,
  SUM(target_1_actions.advertiser_amount) * -0.01 as spend;

target_1_ecpm_cvr_cross = CROSS target_1_impressions_overall, target_1_actions_overall;

target_1_ecpm_cvr_overall = foreach target_1_ecpm_cvr_cross generate
  target_1_impressions_overall::impressions as impressions,
  target_1_actions_overall::conversions as conversions,
  target_1_actions_overall::spend as spend,
  (float)target_1_actions_overall::conversions/(float)target_1_impressions_overall::impressions as cvr,
  (float)target_1_actions_overall::spend/(float)target_1_impressions_overall::impressions * 1000 as ecpm;

target_1_ecpm_cvr_offer = foreach (join target_1_impressions_offer by offer_id left, target_1_actions_offer by offer_id) generate
  target_1_impressions_offer::offer_id as offer_id,
  target_1_impressions_offer::impressions as impressions,
  (target_1_actions_offer::conversions is null ? 0 : target_1_actions_offer::conversions) as conversions,
  (target_1_actions_offer::spend is null ? 0 : target_1_actions_offer::spend) as spend,
  (float)(target_1_actions_offer::conversions is null ? 0 : target_1_actions_offer::conversions)/(float)target_1_impressions_offer::impressions as cvr,
  (target_1_actions_offer::spend is null ? 0 : target_1_actions_offer::spend)/(float)target_1_impressions_offer::impressions * 1000 as ecpm;

rmf /user/etl/jbattles/persona_1_ab/ecpm_cvr_overall/d=$d;
rmf /user/etl/jbattles/persona_1_ab/ecpm_cvr_offer/d=$d;

store target_1_ecpm_cvr_overall into '/user/etl/jbattles/persona_1_ab/ecpm_cvr_overall/d=$dir_name' using PigStorage();
store target_1_ecpm_cvr_offer into '/user/etl/jbattles/persona_1_ab/ecpm_cvr_offer/d=$dir_name' using PigStorage();




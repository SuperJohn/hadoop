/*
Description Here
*/

SET JOB.NAME johns_job ;
SET DEFAULT_PARALLEL 50 ;

ow_views_raw = load 'hdfs://hadoop-prod-nn1/user/hive/warehouse/offerwall_views/{d=2015-09-10,d=2015-09-11,d=2015-09-12,d=2015-09-13,d=2015-09-14,d=2015-09-15,d=2015-09-16}' using parquet.pig.ParquetLoader();
ow_views = foreach ow_views_raw generate udid, device_type, device_name, app_id, offer_id, time, source, etl_day;
ow_impressions = filter ow_views by (source == 'offerwall' or source == 'offerwall_from_message' or source == 'offerwall_interstitial' or source == 'offerwall_pubinitiated');
imp_offers_of_interest = filter ow_impressions by offer_id matches '739d56ff-dbb0-4d16-b874-7b37f4b824e4|749691d0-d225-4732-9fe1-d4fdb245ac76|c0d2deef-3b12-4ded-9ee2-01ebcde19dc8|3fa93de8-da86-4570-8d2c-366feba79f87|d17407ca-28eb-4687-8173-a012eb896168|3fb7e684-5f5d-4e17-8afc-b3eb0d1934ae|c325996d-9907-45e3-89a6-4a6acbf5d461|f7b11aed-00b9-4b0e-9a92-1d990190fc87|62aa056e-66b9-4659-9cf8-c387cdfe21d7';
imp_group = group imp_offers_of_interest by (udid, device_type, device_name, app_id, offer_id);
imp_metrics = foreach imp_group generate flatten(group), COUNT(imp_offers_of_interest) as ow_impressions;

--

actions_raw = load 'hdfs://hadoop-prod-nn1/user/hive/warehouse/actions/{d=2015-09-10,d=2015-09-11,d=2015-09-12,d=2015-09-13,d=2015-09-14,d=2015-09-15,d=2015-09-16}' using parquet.pig.ParquetLoader();
actions = foreach actions_raw generate udid, device_type, device_name, publisher_app_id, offer_id, predecessor_offer_id, source, advertiser_amount, etl_day;
ow_actions = filter actions by (source == 'offerwall' or source == 'offerwall_from_message' or source == 'offerwall_interstitial' or source == 'offerwall_pubinitiated');
act_offers_of_interest = filter ow_actions by offer_id matches '739d56ff-dbb0-4d16-b874-7b37f4b824e4|749691d0-d225-4732-9fe1-d4fdb245ac76|c0d2deef-3b12-4ded-9ee2-01ebcde19dc8|3fa93de8-da86-4570-8d2c-366feba79f87|d17407ca-28eb-4687-8173-a012eb896168|3fb7e684-5f5d-4e17-8afc-b3eb0d1934ae|c325996d-9907-45e3-89a6-4a6acbf5d461|f7b11aed-00b9-4b0e-9a92-1d990190fc87|62aa056e-66b9-4659-9cf8-c387cdfe21d7';
act_group = group act_offers_of_interest by (udid, device_type, device_name, publisher_app_id, offer_id);
act_metrics = foreach act_group generate flatten(group), COUNT(act_offers_of_interest) as ow_conversions;

--

metrics = join imp_metrics by (group::udid, group::device_type, group::device_name, group::app_id, group::offer_id) left,
			   act_metrics by (group::udid, group::device_type, group::device_name, group::publisher_app_id, group::offer_id);
metrics_clean = foreach metrics GENERATE imp_metrics::group::udid as udid, imp_metrics::group::device_type as device_type, imp_metrics::group::device_name as device_name, imp_metrics::group::app_id as app_id,
										 imp_metrics::group::offer_id as offer_id, ow_impressions as ow_impressions, ow_conversions as ow_conversions;

ubt = load 'hdfs://10.10.8.5:8020/user/ds/jpark/persona2/ubt/d=2015-08-26' using PigStorage() as (
	date: chararray,
	udid: chararray,
	ratings: chararray
);

metrics_ubt = JOIN metrics_clean BY (udid) LEFT, ubt by (udid);

store metrics_ubt into 'hdfs://hadoop-prod-nn1/user/bi/ksimonds/persona/offer_analysis/query_20150917' using PigStorage();
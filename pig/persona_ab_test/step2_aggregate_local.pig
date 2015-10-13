/* SOME BACKGROUND
*/

SET JOB.NAME johns_persona_data ;
SET DEFAULT_PARALLEL 50 ;

johns_persona_data = load 'part-r-00213.gz' using PigStorage('\t') ;



tracking_logs = load '../data/persona/tracking_logs.gz' using
PigStorage('\\u001') as (
    tracking_id:chararray,
    algoid:chararray,
    control_test:chararray,
    udid:chararray,
    respond_time:chararray,
    respond_status:chararray,
    call_time:chararray,
    exp_label:chararray,
    opt_soa_type:chararray,
    comment:chararray,
    soa_server:chararray,
    etl_day:chararray,
    etl_hour:chararray,
    day:chararray,
    class_lable:chararray,
    class_lable1:chararray,
    class_lable2:chararray,
    class_lable3:chararray,
    source:chararray,
    ad_view_type:chararray )
;
-- SELECT REQUIRED COLUMNS
tracking_data = foreach tracking_logs generate udid, tracking_id, opt_soa_type, control_test;


targeting_logs = load '../data/persona/targeting_logs.gz' using
PigStorage('\\u001') as (
  -- etl_date: chararray,
  holder: chararray,
  optsoa_date: chararray,
  ad_view_id: chararray,
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

/* Filter out some bad data, which causes ad_view_id to be null
*/
targeting_filtered = FILTER targeting_logs  BY publisher_app_id is not null and publisher_app_id != '' ;

-- SELECT REQUIRED COLUMNS
targeting_data = foreach targeting_filtered generate udid, ad_view_id, offer_id, accepted;

-- join the tables
joined = join targeting_data by (udid, ad_view_id)
                    , tracking_data by (udid, tracking_id) ;

/*
results = foreach(group johns_persona_data by (control_type, accepted))
                    generate FLATTEN(group) as (control_type, accepted)
                    , COUNT(johns_persona_data) as count_requests ;
 */

joined_limited = LIMIT joined 10000 ;

rmf ../data/persona/joined/ ;
store joined into '../data/persona/joined/' USING PigStorage('\t', '-schema')  ;

rmf ../data/persona/joined_sample/ ;
store joined_limited into '../data/persona/joined_sample/' USING PigStorage(',', '-schema')  ;

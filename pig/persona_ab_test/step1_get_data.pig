/*
Description Here
*/
set JOB.NAME johns_persona_data;
SET DEFAULT_PARALLEL 50;

-- LOAD TRACKING DATA [exp_label and opt_soa_type were switched]
raw_tracking_logs = load '/user/hive/warehouse/rsyslog_optsoa_tracking/d=2015-10-03/b=*/dummy_server/*.gz' using
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
tracking_data = foreach raw_tracking_logs generate udid, tracking_id, opt_soa_type, control_test;

-- LOAD TARGETING DATA [This schema was completely different than provided. Relabeled colnames.]
raw_targeting_logs = load '/user/etl/ds/optsoa_rsyslog/rsyslog_OptSOATargeting2/d=2015-10-03/*/*/*.gz' usinga
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
targeting_filtered = FILTER raw_targeting_logs  BY publisher_app_id is not null and publisher_app_id != '' ;


-- SELECT REQUIRED COLUMNS
targeting_data = foreach targeting_filtered generate udid, ad_view_id, offer_id, accepted;

/* INNER JOIN TARGETING & TRACKING DATA
 */
joined = join targeting_data by (udid, ad_view_id)
                    , tracking_data by (udid, tracking_id) ;

joined_clean = foreach joined GENERATE tracking_data::udid as udid
                                    , targeting_data::ad_view_id as ad_view_id
                                    , tracking_data::tracking_id as tracking_id
                                    , tracking_data::opt_soa_type as opt_soa_type
                                    , tracking_data::control_test as control_test
                                    , targeting_data::offer_id as offer_id
                                    , targeting_data::accepted as accepted
                                    ;

-- Load lookup table, which provides logic for controled vs exposed, p1.0 vs p2.0
lookup = load 'jhoughton/report/temporary/lookup.txt' using PigStorage(',') as (
    opt_soa_type:chararray
    , control_test:chararray
    , user_type:chararray
    , targeting_version:chararray
    , control_type:chararray
    );

/* INNER JOIN JOINED TABLE TO LOOKUP, ADDING TARGETING TYPE TO EACH RECORD
 */
joined2 = join joined_clean by (opt_soa_type, control_test)
                , lookup by (opt_soa_type, control_test) ;

joined2_clean = foreach joined2 GENERATE joined_clean::udid as udid
                                       , joined_clean::ad_view_id as ad_view_id
                                       , joined_clean::tracking_id as tracking_id
                                       , joined_clean::opt_soa_type as opt_soa_type
                                       , joined_clean::control_test as control_test
                                       , joined_clean::offer_id as offer_id
                                       , joined_clean::accepted as accepted
                                       , lookup::user_type as user_type
                                       , lookup::targeting_version as targeting_version
                                       , lookup::control_type as control_type
                                       ;

-- Create a small limited version, before STORING the large file in HDFS
joined2_limited = LIMIT joined2_clean 10 ;
rmf /user/bi/jhoughton/report/temporary/persona_data1 ;
store joined2_clean into '/user/bi/jhoughton/report/temporary/persona_data1' using PigStorage('\t', '-schema');

DUMP joined2_limited ;


/* Targeting Definitions
new_user x buckets 1,2,3,4,5,6  == targeting 1.0 (control)
new_user x buckets 7,8,9 == targeting 2.0 (test)
old_user x buckets 4,5,6  == targeting 1.0 (control)
old_user x buckets 1,2,3 == targeting 2.0 (test)
 */
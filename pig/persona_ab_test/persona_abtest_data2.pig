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
tracking_data = foreach raw_tracking_logs generate etl_day, udid, tracking_id, opt_soa_type, control_test;

-- LOAD TARGETING DATA [This schema was completely different than provided. Relabeled colnames.]
raw_targeting_logs = load '/user/hive/warehouse/OptSOATargeting2/d=2015-10-03/b=*/*.gz' using PigStorage('\\u001') as (

  etl_date: chararray,
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
targeting_filtered = FILTER raw_targeting_logs  BY publisher_app_id is not null and COLUMN_NAME != '' ;


-- SELECT REQUIRED COLUMNS
targeting_data = foreach raw_targeting_logs generate etl_date, udid, ad_view_id, offer_id, accepted;

/* INNER JOIN TARGETING & TRACKING DATA
 */
joined = join targeting_data by (udid, ad_view_id)
                    , tracking_data by (udid, tracking_id) ;

joined_clean = foreach joined GENERATE tracking_data::etl_day as etl_day
                                    , tracking_data::udid as udid
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

joined2_clean = foreach joined2 GENERATE joined_clean::etl_day as etl_day
                                       , joined_clean::udid as udid
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

-- GROUP TO ACHIEVE RESULTS LIKE PERSONA_VERSION (CONTROL/EXP), OFFER_ID,  ACCEPTED(T/F), COUNT(REQUESTS), COUNT(VIEWS), COUNT(ACTIONS)
results = foreach(group joined2_clean by (etl_day, control_type, offer_id, accepted))
                    generate FLATTEN(group) as (etl_day, control_type, offer_id, accepted)
                    , COUNT(joined2_clean) as count_requests ;

results_limited = LIMIT results 30 ;
rmf /user/bi/jhoughton/report/temporary/persona_data2 ;
store results into '/user/bi/jhoughton/report/temporary/persona_data2' using PigStorage('\t', '-schema');

DUMP joined2_limited ;
DUMP results_limited ;

/* JOIN TO VIEWS TABLE
-- UPDATE THIS!!!
LOAD Views Table d=2015-09-10

joined3 = join joined2_clean by (etl_day, ad_view_id, udid, offer_id)
                , views by (etl_day, ad_view_id, udid, offer_id) ;

joined3_clean = FOREACH joined3 GENERATE joined3::etl_day as etl_day
                                       , joined3::udid as udid
                                       , joined3::ad_view_id as ad_view_id
                                       , joined3::tracking_id as tracking_id
                                       , joined3::opt_soa_type as opt_soa_type
                                       , joined3::control_test as control_test
                                       , joined3::offer_id as offer_id
                                       , joined3::accepted as accepted
                                       , joined3::user_type as user_type
                                       , joined3::targeting_version as targeting_versionf
                                       , joined3::control_type as control_type
                                       , views::source
                                       ;

*/

/* GROUP AND AGGREGATE JOINED DATA FOR INTERMEDIATE RESULTS (accepted counts by controled / exposed)
intermediate_results = foreach(group joined3_clean by (targeting_version, control_type, accepted))
    generate flatten(group), COUNT_STAR() as view_count
     -- CHECK SYNTAX       , COUNT_DISTINCT(joined3_clean.ad_view_id) as adviewid_count
     -- CHECK SYNTAX       , COUNT_DISTINCT(joined3_clean.udid) as user_count
                           ;
*/

/* Targeting Definitions
new_user x buckets 1,2,3,4,5,6  == targeting 1.0 (control)
new_user x buckets 7,8,9 == targeting 2.0 (test)
old_user x buckets 4,5,6  == targeting 1.0 (control)
old_user x buckets 1,2,3 == targeting 2.0 (test)
 */
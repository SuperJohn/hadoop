REGISTER pig/PigUDFJPark.jar;

optsoa_tracking = load 'hdfs://10.10.8.5:8020/user/hive/warehouse/rsyslog_optsoa_tracking/d=$d/b=*/dummy_server/*.gz' using PigStorage('\u0001') as (                           
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
    day2: chararray,
    class_lable: chararray,
    class_lable1: chararray,
    class_lable2: chararray,
    class_lable3: chararray,
    source: chararray,
    ad_view_type: chararray
);

optsoa_tracking_new_old = foreach optsoa_tracking generate 
    tracking_id,
    udid,
    (exp_label is null 
        ? 'unknown' 
        : (LOWER(exp_label)  matches '.*offerwall.new' 
            ? 'new' 
            : (LOWER(exp_label) matches '.*offerwall.old' ? 'old' : 'unknown') 
        ) 
    ) as user_type,
    (exp_label is null or control_test is null 
        ? 'other' 
        :(LOWER(exp_label)  matches '.*offerwall.new' 
            ? ( (control_test == '7' or control_test == '8' or control_test == '9') 
                ? 'test' : 'unknown') 
            : (LOWER(exp_label) matches '.*offerwall.old' 
                ? ( (control_test == '1' or control_test == '2' or control_test == '3') 
                ? 'test' : 'unknown') 
                : 'other')
        )
    ) as control_test;

persona_2 = load 'hdfs://10.10.8.5:8020/user/etl/ds/optsoa_rsyslog/rsyslog_OptSOATargeting2/d=$d/*/*/*.gz' using PigStorage('\u0001') as (
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

persona_2_refined = filter persona_2 by publisher_app_id is not null and publisher_app_id != '';
persona_2_refined2 = filter persona_2_refined by offer_persona is not null and offer_persona != '';

persona_2_data = foreach persona_2_refined2 generate
    udid,
    avi as ad_viewid,
    offer_id,
    offer_persona,
    device_persona,
    com.tapjoy.ds.pig.UDF.IsPersonaMatch( offer_persona, device_persona ) as accepted;

persona_2_targeted = foreach persona_2_data generate 
    udid,
    ad_viewid,
    offer_id,
    offer_persona,
    device_persona,
    (accepted == true ? 'accepted' : 'rejected' ) as accepted;

accept_result = foreach (join persona_2_targeted by ad_viewid, optsoa_tracking_new_old by tracking_id) generate
                persona_2_targeted::ad_viewid as avi,
                persona_2_targeted::udid as udid,
                persona_2_targeted::offer_id as offer_id,
                persona_2_targeted::accepted,
                optsoa_tracking_new_old::user_type,
                optsoa_tracking_new_old::control_test;

accept_result2 = filter accept_result by user_type != 'unknown' and control_test != 'other';
control_accept_result = foreach ( group accept_result2 by (avi, udid, offer_id,user_type,accepted) ) generate
                        FLATTEN(group) as (avi, udid, offer_id,user_type,accepted),
                        COUNT(accept_result2) as freq2;
control_accept_count = foreach ( group control_accept_result by (offer_id,user_type,accepted) ) generate
                       FLATTEN(group) as (offer_id,user_type,accepted),
                       COUNT(control_accept_result) as freq;
/*
control_accept_count = foreach ( group accept_result by (offer_id,user_type,accepted) ) generate
                       FLATTEN(group) as (offer_id,user_type,accepted),
                       COUNT(accept_result) as freq;
*/

store control_accept_count into '/user/ds/jpark/lucid/abtest/ar_2/d=$d' using PigStorage();

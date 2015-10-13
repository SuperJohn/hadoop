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
            ? ( (control_test == '0' or control_test == '1' or control_test == '2' or control_test == '3' or control_test == '4' or control_test == '5' or control_test == '6' ) 
                ? 'control' : 'unknown') 
            : (LOWER(exp_label) matches '.*offerwall.old' 
                ? ( (control_test == '0'  or control_test == '4' or control_test == '5' or control_test == '6') 
                ? 'control' : 'unknown') 
                : 'other')
        )
    ) as control_test;

/*holder:chararray,
date:chararray,
udid:chararray,
ad_viewid:chararray,
primary_offerId:chararray,
initial_index:double,
exact_persona_confidence_score:double,
exact_match_personaid:int,
exact_persona_boost_factor:double,
exact_persona_boost_index:double,
pVal_confidence_score:double,
pVal_personaId:int,
pVal_persona_boost_factor:double,
pVal_persona_boost_index:double,
similar_personaid:chararray,
final_score:double,
is_offer_persona_targeted:boolean"
*/

persona_1 = load 'hdfs://10.10.8.5:8020/user/hive/warehouse/rsyslog_OptSOAPersona/d=$d/part*' using parquet.pig.ParquetLoader();

persona_1_data = foreach persona_1 generate
    udid,
    ad_viewid,
    primary_offerId,
    exact_match_personaid,
    pVal_personaId,
    similar_personaid,
    initial_index,
    pVal_persona_boost_index,
    exact_persona_confidence_score,
    pVal_confidence_score,
    final_score,
    is_offer_persona_targeted;

persona_1_data2 = filter persona_1_data by is_offer_persona_targeted == true;

persona_1_targeted = foreach persona_1_data2 generate 
    ad_viewid,
    udid,
    primary_offerId,
    pVal_confidence_score,
    (pVal_confidence_score is not null 
        ? 'accepted' : 'rejected' ) as accepted;

accept_result = foreach (join persona_1_targeted by ad_viewid, optsoa_tracking_new_old by tracking_id) generate
                persona_1_targeted::ad_viewid as avi,
                persona_1_targeted::udid as udid,
                persona_1_targeted::primary_offerId as offer_id,
                persona_1_targeted::accepted,
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
store control_accept_count into '/user/ds/jpark/lucid/abtest/ar_1/d=$d' using PigStorage();

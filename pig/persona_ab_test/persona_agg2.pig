/* SOME BACKGROUND
-- GROUP TO ACHIEVE RESULTS LIKE PERSONA_VERSION (CONTROL/EXP), OFFER_ID,  ACCEPTED(T/F), COUNT(REQUESTS), COUNT(VIEWS), COUNT(ACTIONS)
results = foreach(group joined2_clean by (etl_day, control_type, offer_id, accepted))
                    generate FLATTEN(group) as (etl_day, control_type, offer_id, accepted)
                    , COUNT(joined2_clean) as count_requests ;

results_limited = LIMIT results 30 ;
rmf /user/bi/jhoughton/report/temporary/persona_data2 ;
store results into '/user/bi/jhoughton/report/temporary/persona_data2' using PigStorage('\t', '-schema');
*/

SET JOB.NAME johns_persona_data ;
SET DEFAULT_PARALLEL 50 ;

johns_persona_data = load '/user/bi/jhoughton/report/temporary/persona_data1/*.gz' using PigStorage('\t') ;

results = filter johns_persona_data by accepted != 'true' and accepted != 'false';

/*results = foreach(group johns_persona_data by (control_type, accepted))
                    generate FLATTEN(group) as (control_type, accepted)
                    , COUNT(johns_persona_data) as count_requests ;

results_limited = LIMIT results 50 ;
*/

rmf /user/bi/jhoughton/report/temporary/persona_agg2 ;
store results into '/user/bi/jhoughton/report/temporary/persona_agg2' USING PigStorage('\t')  ;

DUMP results_limited ;
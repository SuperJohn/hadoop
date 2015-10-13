/* SOME BACKGROUND
*/

SET JOB.NAME johns_persona_data ;
SET DEFAULT_PARALLEL 50 ;

johns_persona_data = load '/user/bi/jhoughton/report/temporary/persona_data1/*.gz' using PigStorage('\t') ;

/*
results = filter johns_persona_data by accepted != 'true' and accepted != 'false';
 */

results = foreach(group johns_persona_data by (control_type, accepted))
                    generate FLATTEN(group) as (control_type, accepted)
                    , COUNT(johns_persona_data) as count_requests ;

results_limited = LIMIT results 50 ;

rmf /user/bi/jhoughton/report/temporary/persona_agg2 ;
store results into '/user/bi/jhoughton/report/temporary/persona_agg2' USING PigStorage('\t')  ;

DUMP results_limited ;
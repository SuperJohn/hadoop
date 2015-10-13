select

char_length(publisher_user_id) as id_length
, count(distinct publisher_user_id) as distinct_userid
, count(*) as count_star
, round(count(distinct publisher_user_id) / count(*),2) as pct_unique

from actions
where day = '2015-09-10'

group by char_length(publisher_user_id)
order by char_length(publisher_user_id) desc


select 

a.app_id
, a.app_name
, a.partner_id
, a.partner_name
, count(DISTINCT b.udid) as users

FROM ( 
        select
            v.app_id,
            ap.app_name,
            ap.partner_id,
            ap.partner_name,
            case
                when cu.callback_url ~~* '%TAP_POINTS_CURRENCY%' then 'Managed'
                else 'Non-managed'
            end as currency_type,
            count(v.udid) as sdk11_2_0_views
        from
            analytics.views v
        join
            analytics.apps_partners ap
                on v.app_id = ap.app_id
        join
            analytics.currencies cu
                on v.currency_id = cu.currency_id
        where
            v.day = '2015-10-03'
            and v.library_version = '11.2.0'
            and ap.app_platform = 'android'
            --and not cu.callback_url ~~* '%TAP_POINTS_CURRENCY%'
        group by 1, 2, 3, 4, 5
        order by 6 desc
       ) a 
       
       JOIN views b on a.app_id = b.app_id
       WHERE b.day = '2015-10-03'
       
group by a.app_id
, a.app_name
, a.partner_id
, a.partner_name
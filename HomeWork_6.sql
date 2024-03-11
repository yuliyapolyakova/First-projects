create or replace function pg_temp.decode_url_part(p varchar) returns varchar as $$
select 
	convert from (
		cast (E'\\x' || string_agg(
			case when length (r.m[1]0 = then encode (convert to (r.m[1], 'SQL_ASCII'), 'hex')
			else substring (r.m[1]) from 2 for 2) end, '') as bytea),'UTF8'
			)
from regexp_matches($1, '%[0-9a-f]|.', 'gi') as r(m);
		
with f_g_bd as(
select ad_date, 
	url_parameters, 
	coalesce (spend, 0) as spend,
	coalesce (impressions, 0) as impressions, 
	coalesce (reach, 0) as reach, 
	coalesce (clicks,0) as clicks, 
	coalesce (leads,0) as leads, 
	coalesce (value,0) as value
from facebook_ads_basic_daily fabd 
union all
select ad_date, 
	url_parameters, 
	coalesce (spend, 0) as spend,
	coalesce (impressions, 0) as impressions, 
	coalesce (reach, 0) as reach, 
	coalesce (clicks,0) as clicks, 
	coalesce (leads,0) as leads, 
	coalesce (value,0) as value
from google_ads_basic_daily gabd 
)
select ad_date,
	case 
	when lower (substring (url_parameters, 'utm_campaign=([\w|\d]+)')) = 'nan' then null
	else lower (substring (url_parameters, 'utm_campaign=([\w|\d]+)'))
	end as utm_campaign,
		sum (spend) as sum_spend,
		sum (impressions) sum_impressions,
		sum (clicks) as sum_clicks,
		sum (value) as sum_value, 
	case when (sum (impressions)) = '0' then '0'
	else round (sum(clicks)::numeric /sum(impressions)*100, 2)
	end as ctr,
	case when (sum (clicks)) = '0' then '0'
	else round (sum(spend)::numeric /sum(clicks), 2)
	end as cpc,
	case when (sum (impressions)) = '0' then '0'
	else 1000 * sum(spend)/sum(impressions)
	end as cpm,
	case when (sum (spend)) = '0' then '0'
	else round((sum(value) :: numeric) /sum(spend) *100, 2)
	end as romi,
		decode_url_part(url_parameters)
from f_g_bd
group by f_g_bd.ad_date, f_g_bd.url_parameters
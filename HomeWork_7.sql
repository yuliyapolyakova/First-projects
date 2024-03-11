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
),
second_db as (
select ad_date,
	date_trunc('month', ad_date) as ad_month, 
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
	end as romi
from f_g_bd
group by f_g_bd.ad_date, f_g_bd.url_parameters),
result_db as (
select date_trunc('month', ad_date) as ad_month,
	utm_campaign,
	sum_spend,
	sum_impressions,
	sum_clicks,
	sum_value,
	round((sum_value :: numeric /sum_spend) *100, 2) as romi,
	sum_spend/sum_clicks as cpc,
	1000 * sum_spend/sum_impressions as cpm,
	round (((sum_clicks) :: numeric / sum_impressions) *100, 2)  as ctr
from second_db
where sum_clicks > 0 and sum_impressions > 0
group by date_trunc('month', ad_date), second_db.utm_campaign, second_db.sum_spend, second_db.sum_impressions, second_db.sum_clicks, second_db.sum_value, utm_campaign),
lag_stat as (
select ad_month,
	utm_campaign,
	cpm,
	ctr,
	romi,
	lag (cpm,1) over (partition by utm_campaign order by ad_month) as prev_cpm,
	cpm - lag (cpm,1) over (partition by utm_campaign order by ad_month) as diff_cpm,
	lag (ctr,1) over (partition by utm_campaign order by ad_month) as prev_ctr,
	ctr - lag (ctr,1) over (partition by utm_campaign order by ad_month) as diff_ctr,
	lag (romi,1) over (partition by utm_campaign order by ad_month) as prev_romi,
	romi - lag (romi,1) over (partition by utm_campaign order by ad_month) as diff_romi
from result_db
group by ad_month, utm_campaign, result_db.cpm,result_db.ctr, result_db.romi)
select ad_month,
utm_campaign,
round ((diff_cpm::numeric/cpm::numeric *100),2) ||'%' as diff_cpm_percent,
round ((diff_ctr::numeric/ctr::numeric *100),2) ||'%' as diff_ctr_percent,
round ((diff_romi::numeric/romi::numeric *100),2) ||'%' as diff_romi_percent
from lag_stat

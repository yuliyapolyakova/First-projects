with facebook_google_ads_cte as (
	select 
		ad_date,
		'Facebook Ads':: text as media_sourse,
		spend,
		impressions,
		reach,
		clicks,
		leads,
		value
	from facebook_ads_basic_daily 
	union all
	select 
		ad_date,
		'Google Ads' ::text as madia_sourse,
		spend,
		impressions,
		reach,
		clicks,
		leads,
		value
	from google_ads_basic_daily)
	select ad_date,
		media_sourse,
		sum(spend) as total_spend,
		sum(impressions) as total_imp,
		sum(clicks) as total_clicks,
		sum (value) as total_value
	from facebook_google_ads_cte
	where ad_date notnull 
	group by ad_date, media_sourse
	order by ad_date desc 



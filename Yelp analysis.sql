-- --UDF 

-- CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
-- RETURNS STRING
-- LANGUAGE PYTHON
-- RUNTIME_VERSION = '3.8'
-- PACKAGES = ('textblob') 
-- HANDLER = 'sentiment_analyzer'
-- AS $$
-- from textblob import TextBlob
-- def sentiment_analyzer(text):
--     analysis = TextBlob(text)
--     if analysis.sentiment.polarity > 0:
--         return 'Positive'
--     elif analysis.sentiment.polarity == 0:
--         return 'Neutral'
--     else:
--         return 'Negative'
-- $$;


-- creating table for business

create or replace table tbl_yelp_business as 
select review_text:business_id::string as business_id
,review_text:name::string as name
,review_text:city::string as city
,review_text:state::string as state
,review_text:review_count::string as review_count
,review_text:stars::number as stars
,review_text:categories::string as categories
from yelp_business limit 100

----- creating review table
create or replace table tbl_yelp_reviews as 
select review_text:business_id::string as business_id ,
review_text:date::date as review_date ,
review_text:user_id::string as user_id,
review_text:stars::number as review_stars,
review_text:text::string as review_text,
analyze_sentiment(review_text) as sentiments
from yelp_reviews 


--- find number of business in each category
with cte as (
select business_id , trim(A.value) as category
from tbl_yelp_business
join lateral split_to_table(categories, ',') A
)

select category, count(*) as no_of_business
from cte
group by category
order by no_of_business desc

-- find the top 10 users who have reviewed the most business in the resturant category 


select r.user_id , count(distinct r.business_id) as no_business
from tbl_yelp_reviews r
inner join tbl_yelp_business b on r.business_id = b.business_id
where b.categories ilike '%restaurant%'
group by r.user_id
order by no_business desc
limit 10



-- find the most popular categories of business 
with cte as (
select business_id , trim(A.value) as category
from tbl_yelp_business
join lateral split_to_table(categories, ',') A
)

select category,count(*) as no_of_reviews
from cte
inner join tbl_yelp_reviews r on cte.business_id = r.business_id
group by category
order by no_of_reviews desc

-- find the top 3 most recent reviews for each business 

with cte as (
select r.* , b.name

, row_number() over (partition by r.business_id order by review_date desc) as  rn

from tbl_yelp_reviews r
inner join tbl_yelp_business b on r.business_id = b.business_id
where b.categories ilike '%restaurant%'
)

select * 
from cte 
where rn<=3


-- find the month with the highest no of reviews 
select month(review_date) as review_month , count(*) as no_of_reiews
from tbl_yelp_reviews
group by review_month
order by no_of_reiews desc 


-- find the percentage of 5-star reviews for each business 
select b.business_id , b.name , count(*) as total_review ,
count( case when r.review_stars = 5 then 1 else null end) as five_star , (five_star/total_review)*100 as perc

from tbl_yelp_reviews r
inner join tbl_yelp_business b 
on r.business_id = b.business_id
group by b.business_id , b.name

--- find the top 5 most reviewed business in each city

with cte as (
select b.city , b.business_id , b.name , count(*) as total_review
from tbl_yelp_reviews r 
inner join tbl_yelp_business b on r.business_id = b.business_id
group by  b.city , b.business_id , b.name
)

select * 
from cte 
qualify row_number () over (partition by city order by total_review desc) <= 5

-- list the top 10 users who have written the most review , along with the business

with cte as (
select r.user_id , count(*) as total_reviews
from tbl_yelp_reviews r 
inner join tbl_yelp_business b on r.business_id = b.business_id
group by  r.user_id
order by total_reviews desc
limit 10
)

select user_id , business_id
from tbl_yelp_reviews 
where user_id in (select user_id from cte)
order by user_id



-- find the top 10 business with highest positive sentiment reviews
 select r.business_id , b.name , count(*) as total_review
 from tbl_yelp_reviews r
 join tbl_yelp_business b 
 on b.business_id = r.business_id
 where sentiments = 'Positive'
 group by  1,2
 order by 3 desc

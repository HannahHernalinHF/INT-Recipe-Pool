----- GB Recipe Pool -----

with scores as(
select region as country,mainrecipecode
    ,sum(score*rating_count)/sum(rating_count) as scorewoscm
    ,sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    ,sum(count_of_1s)/sum(rating_count) as share1
    ,sum(count_of_1s_wscm)/sum(rating_count_wscm) as share1scm
    from(
select *
    ,dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
from materialized_views.gamp_recipe_scores
where region='GB' and score>0 and rating_count>100
) t where o=1
group by 1,2
)

,volumes as(
select  code, round(avg(last_region_share),4) as volume_share_last, round(avg(last_2_region_share),4) as volume_share_2_last,  sum(last_count) as last_count, sum(last_2_count) as last_2_count
from views_analysts.gamp_recipe_volumes
where region='GB'
group by 1
)
-------
------ UPDATE THE PLANNING INTERVAL !!!!!!!
-------
,seasonality as(
select sku,max(seasonality_score) as seasonality_score from uploads.gp_sku_seasonality where country='GB' and week>='W37' and week<='W65'
group by 1
)
,last_sku_cost as(
select code,  status, avg(price) as price
from materialized_views.procurement_services_staticprices sp
left join materialized_views.procurement_services_culinarysku sku
on sku.id=sp.culinary_sku_id
where  sku.market='gb' and sp.hellofresh_week >= '2022-W37' and sp.hellofresh_week <= '2022-W65' and sp.distribution_center='GR'
group by 1,2
)

, last_recipe as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipes
where remps_instance='GB'
)t where o=1
)

, last_cost as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipecost
where remps_instance='GB'
)t where o=1
)

, last_nutrition AS (
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        FROM remps.recipe_nutritionalinfopp
        WHERE remps_instance='GB'
    ) AS t
    WHERE o = 1)



, last_tag as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_tags
where remps_instance='GB'
)t where o=1
)

, last_tag_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_tags_recipes
where remps_instance='GB'
)t where o=1
)

, last_product as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_producttypes
where remps_instance='GB'
)t where o=1
)

, last_preference as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_recipepreferences
where remps_instance='GB'
)t where o=1
)

, last_preference_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_recipepreferences_recipes
where remps_instance='GB'
)t where o=1
)

, last_hqtag as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_hqtags
where remps_instance='GB'
)t where o=1
)

, last_hqtag_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_hqtags_recipes
where remps_instance='GB'
)t where o=1
)

,hqtag as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.original_name,','),'') as name from last_recipe rr
left join last_hqtag_map m on rr.id= m.recipe_recipes_id
left join last_hqtag rt on rt.id=m.recipetags_hqtags_id
group by 1
)

, preference as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe rr
left join last_preference_map m on rr.id= m.recipe_recipes_id
left join last_preference rp on rp.id=m.recipetags_recipepreferences_id
group by 1
)

,producttype as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe rr
left join last_product rp on rp.id=rr.recipe__product_type
group by 1
)
, tag as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','),'')as name from last_recipe rr
left join last_tag_map m on rr.id= m.recipe_recipes_id
left join last_tag rt on rt.id=m.recipetags_tags_id
group by 1
)
,last_ingredient_group as(
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_ingredientgroup
    WHERE remps_instance='GB'
    ) AS t
WHERE o = 1)

, last_recipe_sku as(
SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_recipeskus
    WHERE remps_instance='GB'
    ) AS t
WHERE o = 1)

,last_sku as(
SELECT *
FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.sku_sku
    WHERE remps_instance='GB'
    ) AS t
WHERE o = 1)


,picklists as(
    select
    uniquerecipecode
    , group_concat(code," | ") as skucode
    , group_concat(display_name," | ") as skuname
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , count(distinct code) as skucount
    , sum(price*quantity_to_order_1p) as cost_1p
    , sum(price*quantity_to_order_2p) as cost_2p
    , sum(price*quantity_to_order_3p) as cost_3p
    , sum(price*quantity_to_order_4p) as cost_4p
    , group_concat(price_missing," | ") as pricemissingskus
    , group_concat(price_missing_name," | ") as pricemissingskunames
    from (
        select
        r.unique_recipe_code as uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , seasonality_score
        , sc.price
        , case when sc.price is null or sc.price=0 then sku.code else NULL end as price_missing
        , case when sc.price is null or sc.price=0 then sku.name else NULL end as price_missing_name
        , rs.quantity_to_order_1p
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_3p
        , rs.quantity_to_order_4p
        from last_recipe r
        join last_ingredient_group ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality s on s.sku=sku.code
        left join last_sku_cost sc on sc.code=sku.code
        group by 1,2,3,4,5,6,7,8,9,10,11) t
    group by 1
)

, inactiveskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(distinct skuname," | ") AS inactiveskus,
        count(distinct skuname) AS inactiveskus_count
    from (
        select
        r.unique_recipe_code as unique_recipe_code
        , sku.code as skucode
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        , seasonality_score
        , sc.price
        , case when sc.price is null or sc.price=0 then sku.code else NULL end as price_missing
        , case when sc.price is null or sc.price=0 then sku.name else NULL end as price_missing_name
        , rs.quantity_to_order_1p
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_3p
        , rs.quantity_to_order_4p
        , sku.status
        from last_recipe r
        join last_ingredient_group ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality s on s.sku=sku.code
        left join last_sku_cost sc on sc.code=sku.code
        where sku.status LIKE '%Inactive%' OR sku.status LIKE '%Archived%'
        group by 1,2,3,4,5,6,7,8,9,10,11,12) t
    GROUP BY 1,2--, skus.code
    )

, donotuseskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(distinct skuname," | ") AS donotuseskus,
        count(distinct skuname) AS donotuseskus_count
    from (
        select
        r.unique_recipe_code as unique_recipe_code
        , sku.code as skucode
        --, sku.display_name as skuname
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        , seasonality_score
        , sc.price
        , case when sc.price is null or sc.price=0 then sku.code else NULL end as price_missing
        , case when sc.price is null or sc.price=0 then sku.name else NULL end as price_missing_name
        , rs.quantity_to_order_1p
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_3p
        , rs.quantity_to_order_4p
        from last_recipe r
        join last_ingredient_group ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku rs
        on ig.id = rs.recipe_sku__ingredient_group
        join remps.sku_sku sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality s on s.sku=sku.code
        left join last_sku_cost sc on sc.code=sku.code
        where sku.display_name LIKE '%DO NOT USE%' AND sku.display_name LIKE '%do not use%'
        group by 1,2,3,4,5,6,7,8,9,10,11) t
    GROUP BY 1,2--, skus.code
    )

 ,spicysku as(
    select
    unique_recipe_code
    , group_concat(code," | ") as skucode
    , group_concat(skuname," | ") as spicy_skus
    , count(distinct skuname) as spicy_sku_count
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , count(distinct code) as skucount
    , sum(price*quantity_to_order_1p) as cost_1p
    , sum(price*quantity_to_order_2p) as cost_2p
    , sum(price*quantity_to_order_3p) as cost_3p
    , sum(price*quantity_to_order_4p) as cost_4p
    , group_concat(price_missing," | ") as pricemissingskus
    , group_concat(price_missing_name," | ") as pricemissingskunames
    from (
        select
        r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        , seasonality_score
        , sc.price
        , case when sc.price is null or sc.price=0 then sku.code else NULL end as price_missing
        , case when sc.price is null or sc.price=0 then sku.name else NULL end as price_missing_name
        , rs.quantity_to_order_1p
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_3p
        , rs.quantity_to_order_4p
        from last_recipe r
        join last_ingredient_group ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality s on s.sku=sku.code
        left join last_sku_cost sc on sc.code=sku.code
        where sku.display_name LIKE '%chilli%' OR sku.display_name LIKE '%chili / chili /chili/ Chili%'
            OR sku.display_name LIKE '%chili%'
            OR sku.display_name LIKE '%Chili%'
            OR sku.display_name LIKE '%chilli%'
            OR sku.display_name LIKE '%Sriracha sauce%'
            OR sku.display_name LIKE '%sriracha%'
            OR sku.display_name LIKE '%Jalapeno, Green, Medium Spicy%'
            OR sku.display_name LIKE '%jalapeno%'
            OR sku.display_name LIKE '%Sriracha Mayo%'
            OR sku.display_name LIKE '%Chorizo Sausage%'
            OR sku.display_name LIKE '%Chili, Dried%'
            OR sku.display_name LIKE '%wasabi%'
            OR sku.display_name LIKE '%karashi%'
        group by 1,2,3,4,5,6,7,8,9,10,11) t
    group by 1 )

, steps as (
    SELECT r.id,
           steps.recipe_id,
           r.unique_recipe_code,
           group_concat(steps.title, " | ") AS step_title,
           group_concat(steps.description," | ") as step_description
    FROM materialized_views.culinary_services_recipe_steps_translations as steps
    JOIN materialized_views.isa_services_recipe_consolidated as r
        ON r.id = steps.recipe_id
    GROUP BY 1,2,3
    )

, all_recipes as(
select * from(
select r.id as rempsid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode as code
       ,r.version
       ,r.status
       ,r.title
        ,concat(r.title,coalesce (r.subtitle,''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,''), coalesce(r.primaryvegetable,'')) as subtitle
       ,case when r.primaryprotein IS NULL OR r.primaryprotein = '' then 'not available' else r.primaryprotein end as primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) as mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) as proteincut
       ,coalesce(r.secondaryprotein,'none') as secondaryprotein
       ,concat(coalesce (r.primaryprotein,''),coalesce(r.secondaryprotein,'none')) as proteins
       ,CASE WHEN r.primarystarch IS NULL OR r.primarystarch = '' THEN 'not available' ELSE r.primarystarch END AS primarystarch
       ,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') as mainstarch
       ,coalesce(r.secondarystarch,'none') as secondarystarch
       ,concat(coalesce (r.primarystarch,''),coalesce(r.secondarystarch,'none')) as starches
       ,case when coalesce(r.primaryvegetable,'none') IS NULL OR coalesce(r.primaryvegetable,'none') = '' then 'not available' else coalesce(r.primaryvegetable,'none') end as primaryvegetable
       ,coalesce(TRIM(coalesce(split_part(r.primaryvegetable,'-',1),r.primaryvegetable)),'none') as mainvegetable
       --,concat(coalesce (r.primaryvegetable,'none'),coalesce(r.secondaryvegetable,'none'),coalesce(r.tertiaryvegetable,'none')) as vegetables
       ,coalesce(r.secondaryvegetable,'none') as secondaryvegetable
       --,coalesce(r.tertiaryvegetable,'none') as tertiaryvegetable
       --,coalesce(r.primarydryspice,'none') as primarydryspice
       ,coalesce(r.primarycheese,'none') as primarycheese
       ,coalesce(r.primaryfruit,'none') as primaryfruit
       ,coalesce(r.primarydairy,'none') as primarydairy
       --,coalesce(r.primaryfreshherb,'none') as primaryfreshherb
       --,coalesce(r.primarysauce,'none') as primarysauce
       ,case when n.salt is null then 0 else n.salt end as salt
       ,case when n.kilo_calories=0 then 999 else n.kilo_calories end as calories
       ,case when n.carbohydrates=0 then 999 else n.kilo_calories end as carbohydrates
       ,case when n.proteins = 0 or n.proteins is null then 999 else n.proteins end as n_proteins
       ,case when r.cuisine IS NULL OR r.cuisine = '' then 'not available' else r.cuisine end as cuisine
       ,case when r.dishtype IS NULL OR r.dishtype = '' then 'not available' else r.dishtype end as dishtype
       ,case when r.handsontime ="" or r.handsontime is NULL then cast(99 as float)
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is NULL then 99
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is not NULL then cast( left(r.handsontime,2) as float)
             when length (r.handsontime) <2 then cast(99 as float)
             when r.handsontime='0' then cast(99 as float)
             else cast(r.handsontime as float) end as handsontime
       ,case when r.totaltime ="" or r.totaltime is NULL then cast(99 as float)
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is NULL then 99
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is not NULL then cast( left(r.totaltime,2) as float)
             when length (r.totaltime) <2 then cast(99 as float)
             when r.totaltime='0' then cast(99 as float)
             else cast(r.totaltime as float) end as totaltime
       ,difficultylevel as difficulty
       --,ht.name as hqtag
       --,rt.name as tag
       ,case when pf.name IS NULL OR pf.name = '' then 'not available' else pf.name end as preference
       ,concat (ht.name,rt.name,pf.name) as preftag
       ,pt.name as producttype
       ,p.skucode
     --,p.skucount
       ,lower(p.skuname) as skuname
       ,p.skucount
       --, sc2p.skucount
       , i.inactiveskus_count
       , d.donotuseskus_count
       , i.inactiveskus
       , d.donotuseskus
       , k.spicy_sku_count
       , k.spicy_skus
       ,p.seasonalityrisk
       --,r.author
       --,round(p.cost_1p,2) as cost1p
       ,round(p.cost_2p,2) as cost2p
       --,round(p.cost_3p,2) as cost3p
       ,round(p.cost_4p,2) as cost4p
       ,p.pricemissingskus
       --,p.pricemissingskunames
      /*,case when s.scorescm is not NULL then s.scorescm
             when avg(s.scorescm) over (partition by r.primaryprotein,r.cuisine, r.country ) is not NULL
                 THEN avg(s.scorescm) over (partition by r.primaryprotein,r.cuisine, r.country )
             when avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country ) is not NULL
                 THEN avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country )
             when avg(s.scorescm) over (partition by r.primaryprotein, r.country ) is not NULL
                 THEN avg(s.scorescm) over (partition by r.primaryprotein, r.country)
             when avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                 THEN avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country)
            else 3.4
            end as scorescm
      ,case when s.scorewoscm is not NULL then s.scorewoscm
            when avg(s.scorewoscm) over (partition by r.primaryprotein,r.cuisine, r.country ) is not NULL
                THEN avg(s.scorewoscm) over (partition by r.primaryprotein,r.cuisine, r.country )
            when avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country ) is not NULL
                THEN avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country )
            when avg(s.scorewoscm) over (partition by r.primaryprotein, r.country ) is not NULL
                THEN avg(s.scorewoscm) over (partition by r.primaryprotein, r.country)
            when avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                THEN avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country)
            else 3.4
            end as scorewoscm*/
     --,case when s.scorewoscm is  NULL then 1 else 0 end as isscorereplace
      /*,case when s.share1 is not NULL then s.share1
             when avg(s.share1) over (partition by r.primaryprotein,r.cuisine, r.country ) is not NULL
                 THEN avg(s.share1) over (partition by r.primaryprotein,r.cuisine, r.country )
             when avg(s.share1) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country ) is not NULL
                 THEN avg(s.share1) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country )
             when avg(s.share1) over (partition by r.primaryprotein, r.country ) is not NULL
                 THEN avg(s.share1) over (partition by r.primaryprotein, r.country)
             when avg(s.share1) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                 THEN avg(s.share1) over (partition by split_part(r.primaryprotein,'-',1), r.country)
            else 0
            end as share1
      ,case when s.share1scm is not NULL then s.share1scm
            when avg(s.share1scm) over (partition by r.primaryprotein,r.cuisine, r.country ) is not NULL
                THEN avg(s.share1scm) over (partition by r.primaryprotein,r.cuisine, r.country )
            when avg(s.share1scm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country ) is not NULL
                THEN avg(s.share1scm) over (partition by split_part(r.primaryprotein,'-',1),r.cuisine, r.country )
            when avg(s.share1scm) over (partition by r.primaryprotein, r.country ) is not NULL
                THEN avg(s.share1scm) over (partition by r.primaryprotein, r.country)
            when avg(s.share1scm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                THEN avg(s.share1scm) over (partition by split_part(r.primaryprotein,'-',1), r.country)
            else 0
            end as share1scm
     ,coalesce(v.volume_share_last,0) as volumesharelast
     ,coalesce(v.volume_share_2_last,0) as volumeshare2last*/
     ,r.lastused
     --,r.nextused
     ,r.absolutelastused
     ,case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end as isnewrecipe
     --,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
     ,r.isdefault as isdefault
     ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
     ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
from materialized_views.int_scm_analytics_remps_recipe as r
left join last_recipe r2 on r2.unique_recipe_code=r.uniquerecipecode
left join last_cost rc on rc.id=r2.recipe__recipe_cost
left join last_nutrition n on n.id=r.nutritionalinfo2p
left join scores s on s.mainrecipecode=r.mainrecipecode and s.country=r.country
left join picklists p on p.uniquerecipecode=r.uniquerecipecode
left join preference as pf on pf.uniquerecipecode=r.uniquerecipecode
left join hqtag as ht on ht.uniquerecipecode=r.uniquerecipecode
left join tag as rt on rt.uniquerecipecode=r.uniquerecipecode
left join producttype as pt on pt.uniquerecipecode=r.uniquerecipecode
left join volumes v on v.code=r.mainrecipecode
left join inactiveskus as i on p.uniquerecipecode = i.unique_recipe_code --and on p.skucode = i.skucode
left join donotuseskus as d on p.uniquerecipecode = d.unique_recipe_code --and on p.skucode = d.skucode
left join spicysku as k on p.uniquerecipecode = k.unique_recipe_code
left join steps ON steps.unique_recipe_code = r.uniquerecipecode
where lower(r.status) in ('ready for menu planning','final cook','in development')
    and rc.cost_2p >1.5
    and rc.cost_3p >0
    and rc.cost_4p >0
    and lower(r.title) not like '%not use%' and lower(r.title) not like '%wrong%' and lower(r.title) not like '%test%' and lower(r.title) not like '%brexit%'
    and length (primaryprotein)>0 and primaryprotein <>'White Fish - Coley'
    and primaryprotein <>'N/A'
    and r.uniquerecipecode not like '%MOD%' and r.uniquerecipecode not like '%ASD%' --and r.uniquerecipecode not like 'GC%'
    and pt.name not in  ('add-on', 'Baking kits','Breakfast', 'Sides', 'Dessert', 'Bread','Brunch','Cheese', 'Desserts', 'Modularity', 'Ready Meals','Speedy lunch', 'Speedy Lunch' ,'Soup')
    and r.title not in ('add-on')
    and  r.country='GB'
) temp
where o=1)

select distinct * from all_recipes
WHERE uniquerecipecode NOT LIKE '%AO-%'
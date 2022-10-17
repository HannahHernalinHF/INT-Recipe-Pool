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
select sku,max(seasonality_score) as seasonality_score from uploads.gp_sku_seasonality where country='GB' --and week>='W37' and week<='W65'
group by 1
)

--- region_code = 'GB' empty
, recipe_usage as(
select * from materialized_views.isa_services_recipe_usage
where market = 'gb'
)

--- nutrition = last_nutrition
--- add market - do we need it?
, nutrition as(
select *
from materialized_views.culinary_services_recipe_segment_nutrition
where country = 'GB' and market = 'gb'
)

--- cost = last_cost
--- check distribution center
,cost as(
select recipe_id, size, avg(price) as cost
from materialized_views.culinary_services_recipe_static_price sp
where sp.segment='GR' --and sp.hellofresh_week >= '{weekstart}' and sp.hellofresh_week <= '{weekend}' and sp.distribution_center='GR'
group by 1,2
)

--- sku_cost = last_sku_cost
--- check distribution center
, sku_cost as(
select code, avg(price) as price
from materialized_views.procurement_services_staticprices sp
left join materialized_views.procurement_services_culinarysku sku
on sku.id=sp.culinary_sku_id
where  sku.market='gb' --and sp.hellofresh_week >= '{weekstart}' and sp.hellofresh_week <= '{weekend}' and sp.distribution_center='GR'
group by 1
)

--- AIP
,aip1 as (
    select a.hellofresh_week,
           a.recipe_index,
           b.recipe_code,
           a.total_box,
           SUM(a.pc2_accounting) / a.total_box as AIP_ACCOUNTING_PC2
    from materialized_views.incremental_pc2_dashboard_view a
             join materialized_views.isa_services_menu b
                  on (a.hellofresh_week = b.hellofresh_week and CAST(a.recipe_index AS INT) = b.slot_number)
    where a.hellofresh_week >= '2021-W01'
      and a.entity_code = 'HF-UK'
      and a.recipe_type = 'Surcharge'
    group by hellofresh_week, recipe_index, b.recipe_code, total_box
)
, aip2 as (
    select *
         , avg(aip_accounting_pc2) over(partition by recipe_code) avg_aip
    , rank() over(partition by recipe_code order by hellofresh_week desc) last_aip
    from aip1
)

,picklists as(
    select
    unique_recipe_code
    , group_concat(code," | ") as skucode
    , group_concat(name," | ") as skuname
    , group_concat(packaging_type," | ") as skupackaging_type
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , count(distinct code) as skucount
    , sum(cost1p) as cost1p
    , sum(cost2p) as cost2p
    , sum(cost3p) as cost3p
    , sum(cost4p) as cost4p
    , group_concat(price_missing," | ") as pricemissingskus
    from (
        select
         r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') as name
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id and r.market = p.market
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code and p.market = pk.market
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        where r.market = 'gb' and p.segment_name = 'GR'
        group by 1,2,3,4,5,6) t
    group by 1
)

, inactiveskus as (
    SELECT unique_recipe_code,
        group_concat(distinct skucode, " | ") AS inactiveskucodes,
        group_concat(distinct skuname," | ") AS inactiveskus,
        count(distinct skuname) AS inactiveskus_count
    from (
        select
         r.unique_recipe_code
        , p.code as skucode
        , regexp_replace(p.name, '\t|\n', '') as skuname
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id and r.market = p.market
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code and p.market = pk.market
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        where pk.status LIKE '%Inactive%' OR pk.status LIKE '%Archived%'
        group by 1,2,3,4,5,6) t
    GROUP BY 1
    )

, donotuseskus as (
    SELECT unique_recipe_code,
        group_concat(distinct skucode," | ") AS donotuseskucodes,
        group_concat(distinct skuname," | ") AS donotuseskus,
        count(distinct skuname) AS donotuseskus_count
    from (
        select
         r.unique_recipe_code
        , p.code as skucode
        , regexp_replace(p.name, '\t|\n', '') as skuname
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id and r.market = p.market
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code and p.market = pk.market
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        where p.name LIKE '%DO NOT USE%' AND p.name LIKE '%do not use%'
        group by 1,2,3,4,5,6) t
    GROUP BY 1
    )

 ,spicysku as(
    select
    unique_recipe_code
    , group_concat(skucode," | ") as spicy_skucode
    , group_concat(skuname," | ") as spicy_skus
    , count(distinct skuname) as spicy_sku_count
    from (
        select
         r.unique_recipe_code
        , p.code as skucode
        , regexp_replace(p.name, '\t|\n', '') as skuname
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code and p.market = pk.market
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        where p.name LIKE '%chilli%'
        --p.name LIKE '%chilli%' OR p.name LIKE '%chili / chili /chili/ Chili%'
            --OR p.name LIKE '%Chili%'
            --OR p.name LIKE '%chilli%'
            --OR p.name LIKE '%Sriracha sauce%'
            --OR p.name LIKE '%sriracha%'
            --OR p.name LIKE '%Jalapeno, Green, Medium Spicy%'
            --OR p.name LIKE '%jalapeno%'
            --OR p.name LIKE '%Sriracha Mayo%'
            --OR p.name LIKE '%Chorizo Sausage%'
            --OR p.name LIKE '%Chili, Dried%'
            --OR p.name LIKE '%wasabi%'
            --OR p.name LIKE '%karashi%'
            AND p.market = 'gb'
        group by 1,2,3,4,5,6) t
    group by 1 )

/*
, skucount_2p as(
    select
    uniquerecipecode
    , group_concat(skucode," | ") as skucode
    , group_concat(skuname," | ") as skuname
    --, size
    from (
        select
         r.unique_recipe_code as uniquerecipecode
        , p.code as skucode
        , regexp_replace(p.name, '\t|\n', '') as skuname
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id and r.market = p.market
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code and p.market = pk.market
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        --left join remps.picklists as rp on rp.recipe_id = r.id
        --left join last_sku_cost sc on sc.code=sku.code
        --where rp.size IN (2,4)
        group by 1,2,3,4,5,6) t
    group by 1
)*/

 , skucount_2p as (
        SELECT unique_recipe_code
                , group_concat(NAME, " | ") AS skuname
                , count(distinct code) AS skucount
                , group_concat(status, " | ") as sku_status
                , size
        FROM (
            SELECT r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , skus.status
                , p.size
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
               ON skus.id = p.culinarysku_id
            WHERE r.market = 'gb'
            AND p.segment_name ='GR'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5) t
        GROUP BY 1,5
    )

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
select r.id as uuid
       ,case when r.market='gb' then 'GB' else r.market end as country
       ,r.unique_recipe_code
       ,r.recipe_code as code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') as title
       ,concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
       ,case when r.primary_protein IS NULL OR r.primary_protein = '' then 'not available' else  r.primary_protein end as primaryprotein
       ,r.main_protein as mainprotein
       ,r.protein_cut as proteincut
       ,coalesce(r.secondary_protein,'none') as secondaryprotein
       ,r.proteins
       ,case when r.primary_starch IS NULL OR r.primary_protein = '' then 'not available' else r.primary_starch end as primarystarch
       ,r.main_starch as mainstarch
       ,coalesce(r.secondary_starch,'none') as secondarystarch
       ,r.starches
       ,coalesce(r.primary_vegetable,'none') as primaryvegetable
       ,r.main_vegetable as mainvegetable
       --,concat(coalesce (r.primaryvegetable,'none'),coalesce(r.secondaryvegetable,'none'),coalesce(r.tertiaryvegetable,'none')) as vegetables
       ,coalesce(r.secondary_vegetable,'none') as secondaryvegetable
       --,coalesce(r.tertiaryvegetable,'none') as tertiaryvegetable
       --,coalesce(r.primarydryspice,'none') as primarydryspice
       ,coalesce(r.primary_cheese,'none') as primarycheese
       ,coalesce(r.primary_fruit,'none') as primaryfruit
       ,coalesce(r.primary_dairy,'none') as primarydairy
       --,coalesce(r.primaryfreshherb,'none') as primaryfreshherb
       --,coalesce(r.primarysauce,'none') as primarysauce
       ,case when n.salt is null then 0 else n.salt end as salt
       ,case when n.energy = 0 or n.energy is null then 999 else n.energy end as calories
       ,case when n.carbs=0 then 999 else n.carbs end as carbohydrates
       ,case when n.proteins = 0 or n.proteins is null then 999 else n.proteins end as n_proteins
       ,case when r.cuisine IS NULL OR r.cuisine = '' then 'not available' else r.cuisine end as cuisine
       ,case when r.dish_type IS NULL OR r.dish_type = '' then 'not available' else r.dish_type end as dishtype
       ,case when r.hands_on_time ="" or r.hands_on_time is NULL then cast(99 as float)
             when length (r.hands_on_time) >3 and cast( left(r.hands_on_time,2) as float) is NULL then 99
             when length (r.hands_on_time) >3 and cast( left(r.hands_on_time,2) as float) is not NULL then cast( left(r.hands_on_time,2) as float)
             when length (r.hands_on_time) <2 then cast(99 as float)
             when r.hands_on_time='0' then cast(99 as float)
             else cast(r.hands_on_time as float) end as handsontime
       ,case when r.total_time ="" or r.total_time is NULL then cast(99 as float)
             when length (r.total_time) >3 and cast( left(r.total_time,2) as float) is NULL then 99
             when length (r.total_time) >3 and cast( left(r.total_time,2) as float) is not NULL then cast( left(r.total_time,2) as float)
             when length (r.total_time) <2 then cast(99 as float)
             when r.total_time='0' then cast(99 as float)
             else cast(r.total_time as float) end as totaltime
       ,r.difficulty
       --,ht.name as hqtag
       --,rt.name as tag
       ,case when r.target_preferences IS NULL OR r.target_preferences = '' then 'not available' else r.target_preferences end as preference
       ,concat (r.tags,r.target_preferences) as preftag
       ,r.recipe_type as recipetype
       ,p.skucode
       ,lower(p.skuname) as skuname
       --,p.skucount
       , sc2p.skucount
       , i.inactiveskus_count
       , d.donotuseskus_count
       , i.inactiveskus
       , d.donotuseskus
       , k.spicy_sku_count
       , k.spicy_skus
       ,p.seasonalityrisk
       --,r.author
       --,round(p.cost_1p,2) as cost1p
       ,round(p.cost2p,2) as cost2p
       --,round(p.cost_3p,2) as cost3p
       ,round(p.cost4p,2) as cost4p
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
     ,u.last_used as lastused
     --,r.nextused
     ,case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
     ,coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
     --,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
     ,r.is_default as isdefault
     ,dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
     ,r.updated_at as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     --,coalesce(a.AIP_ACCOUNTING_PC2,0) as last_aip
     --,coalesce(a.avg_aip,0) as avg_aip
from materialized_views.isa_services_recipe_consolidated as r
left join recipe_usage u on u.recipe_code = r.recipe_code
left join nutrition n on n.recipe_id = r.id
left join (select * from cost where size=1) rc_1 on rc_1.recipe_id=r.id
left join (select * from cost where size=2) rc_2 on rc_2.recipe_id=r.id
left join (select * from cost where size=3) rc_3 on rc_3.recipe_id=r.id
left join (select * from cost where size=4) rc_4 on rc_4.recipe_id=r.id
left join scores s on s.mainrecipecode=r.recipe_code
left join picklists p on p.unique_recipe_code=r.unique_recipe_code
left join volumes v on v.code=r.recipe_code
left join (select * from aip2 where last_aip = 1) a on a.recipe_code=r.recipe_code
left join inactiveskus as i on p.unique_recipe_code= i.unique_recipe_code --and on p.skucode = i.skucode
left join donotuseskus as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join spicysku as k on p.unique_recipe_code = k.unique_recipe_code
left join steps ON steps.unique_recipe_code = r.unique_recipe_code
left join skucount_2p as sc2p on sc2p.unique_recipe_code=r.unique_recipe_code
where lower(r.status) in ('ready for menu planning','final cook')
    and p.cost2p >1.5
    and p.cost3p >0
    and p.cost4p >0
        and  lower(r.title) not like '%not use%' and lower(r.title) not like '%wrong%' and lower(r.title) not like '%test%' and lower(r.title) not like '%brexit%'
        and length (r.primary_protein)>0 and r.primary_protein <>'White Fish - Coley'
        and r.primary_protein <>'N/A'
        and r.unique_recipe_code not like '%MOD%' and r.unique_recipe_code not like '%ASD%' and r.unique_recipe_code not like 'GC%' and r.unique_recipe_code not like 'A%' and r.unique_recipe_code not like 'X%'
        and r.target_products not in  ('add-on', 'Baking kits','Breakfast', 'Sides', 'Dessert', 'Bread','Brunch','Cheese', 'Desserts', 'Modularity', 'Ready Meals','Speedy lunch', 'Speedy Lunch' ,'Soup')
        and r.market='gb'
        and r.is_default=true
) temp
where o=1)

select distinct * from all_recipes
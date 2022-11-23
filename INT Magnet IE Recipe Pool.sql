----- IE Magnet Recipe Pool Query -----

with seasonality as(
select sku,max(seasonality_score) as seasonality_score from uploads.gp_sku_seasonality where country='IE' --and week>='W41'and week<='W46'
group by 1
)

, recipe_usage as(
select * from materialized_views.isa_services_recipe_usage
where region_code='ie' and market = 'ie'
)

, nutrition as(
select *
from materialized_views.culinary_services_recipe_segment_nutrition
where segment = 'IE' and market = 'ie'
)

,cost as(
select recipe_id, size, avg(price) as cost
from materialized_views.culinary_services_recipe_static_price sp
where segment='IE' and sp.distribution_center='IE' --and hellofresh_week >= '2022-W41' and hellofresh_week <= '2022-W46'
group by 1,2
)

, sku_cost as(
select code,  avg(price) as price
from materialized_views.procurement_services_staticprices sp
left join materialized_views.procurement_services_culinarysku sku
on sku.id=sp.culinary_sku_id
where  sku.market='ie' and sp.distribution_center='IE' --and sp.hellofresh_week >= '2022-W41' and sp.hellofresh_week <= '2022-W46'
group by 1
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
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code
        left join sku_cost c on c.code = p.code
        left join seasonality s on s.sku = p.code
        where r.market = 'ie' and p.segment_name = 'IE'
        group by 1,2,3,4,5,6) t
    group by 1
)

, inactiveskus_INT as (
    SELECT market,
        segment_name,
        unique_recipe_code,
        group_concat(skucode," | ") AS inactiveskuscodes,
        group_concat(skuname," | ") AS inactiveskus,
        count(skuname) AS inactiveskus_count
    FROM (
            SELECT r.market
                , p.segment_name
                , r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , skus.status
                , p.size
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
              ON skus.id = p.culinarysku_id
            WHERE r.market IN ('dkse','it','jp','ie')
            AND p.segment_name IN ('SE', 'NO', 'IT','JP','IE')
            AND skus.status LIKE '%Inactive%' OR skus.status LIKE '%Archived%'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 7--, 11, 12
         ) t
    GROUP BY 1,2,3--, skus.code
    )

, donotuseskus_INT as (
    SELECT market,
        segment_name,
        unique_recipe_code,
        group_concat(skucode," | ") AS donotuseskuscodes,
        group_concat(skuname," | ") AS donotuseskus,
        count(skuname) AS donotuseskus_count
    FROM (
            SELECT r.market
                , p.segment_name
                , r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , skus.status
                , p.size
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
               ON skus.id = p.culinarysku_id
            WHERE r.market IN ('dkse','it','jp','ie')
            AND p.segment_name IN ('SE','NO','IT','JP','IE')
            AND p.name LIKE '%DO NOT USE%' OR p.name LIKE '%do not use%'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 7--, 11, 12
         ) t
    GROUP BY 1,2,3
    )

, spicysku_INT as (
    SELECT market,
           segment_name,
           unique_recipe_code,
           count(distinct skuname) AS spicy_sku_count,
           group_concat(distinct skuname, " | ") AS spicy_skus
    FROM (
            SELECT r.market
                , p.segment_name
                , r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , skus.status
                , p.size
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
               ON skus.id = p.culinarysku_id
            WHERE r.market IN ('dkse','it','jp','ie')
            AND p.segment_name IN ('SE', 'NO', 'IT','JP','IE')
            AND lower(p.name) LIKE '%chili%'
                 OR lower(p.name) LIKE '%chilli%'
                 OR lower(p.name) LIKE '%sriracha%'
                 OR lower(p.name) LIKE '%jalapeno%'
                 OR lower(p.name) LIKE '%chorizo sausage%'
                 OR lower(p.name) LIKE '%wasabi%'
                 OR lower(p.name) LIKE '%karashi%'
            GROUP BY 1, 2, 3, 4, 5, 6, 7--, 11, 12
         ) t
    GROUP BY 1,2,3    )


, steps_INT as (
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

, all_recipes_IE as(
select * from(
select r.id as uuid
       ,upper(r.market) as country
       ,r.unique_recipe_code as uniquerecipecode
       ,r.recipe_code as code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') as title
       ,concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
       ,case when r.primary_protein IS NULL or r.primary_protein = "" then 'not available' else r.primary_protein end as primaryprotein
       ,r.main_protein as mainprotein
       ,r.protein_cut as proteincut
       ,coalesce(r.secondary_protein,'none') as secondaryprotein
       ,r.proteins
       ,case when r.primary_starch IS NULL or r.primary_starch = '' then 'not available' else r.primary_starch end as primarystarch
       ,r.main_starch as mainstarch
       ,coalesce(r.secondary_starch,'none') as secondarystarch
       ,r.starches
       ,case when coalesce(r.primary_vegetable,'none') IS NULL or coalesce(r.primary_vegetable,'none') = '' then 'not available' else r.primary_protein end as primaryvegetable
       ,r.main_vegetable as mainvegetable
       --,r.vegetables
       ,coalesce(r.secondary_vegetable,'none') as secondaryvegetable
       --,coalesce(r.tertiary_vegetable,'none') as tertiaryvegetable
       --,coalesce(r.primary_dry_spice,'none') as primarydryspice
       ,coalesce(r.primary_cheese,'none') as primarycheese
       ,coalesce(r.primary_fruit,'none') as primaryfruit
       ,coalesce(r.primary_dairy,'none') as primarydairy
       --,coalesce(r.primary_fresh_herb,'none') as primaryfreshherb
       --,coalesce(r.sauce_paste,'none') as saucepaste
       ,case when n.salt is null then 0 else n.salt end as salt
       ,case when n.energy = 0 or n.energy is null then 999 else n.energy end as calories
       ,case when n.carbs = 0  or n.carbs is null then 999 else n.carbs end as carbohydrates
       ,case when n.proteins = 0 or n.proteins is null then 999 else n.proteins end as n_proteins
       ,case when r.cuisine IS NULL or r.cuisine = '' then 'not available' else r.cuisine end as cuisine
       ,case when r.dish_type IS NULL or r.dish_type = '' then 'not available' else r.dish_type end as dishtype
       ,case when r.hands_on_time ="" or r.hands_on_time is NULL then cast(99 as float)
             else cast(r.hands_on_time as float) end as handsontime
       ,case when r.hands_on_time ="" or r.hands_on_time is NULL then cast(99 as float)
             else cast(r.hands_on_time as float) end
              +
        case when r.hands_off_time ="" or r.hands_off_time is NULL then cast(99 as float)
             else cast(r.hands_off_time as float) end
              as totaltime
       ,r.difficulty
       --,r.tags as tag
       ,case when r.target_preferences IS NULL or r.target_preferences = '' then 'not available' else r.target_preferences end as preference
       ,concat (r.tags,r.target_preferences) as preftag
       ,r.recipe_type as recipetype
       --,r.target_products as producttype
       ,p.skucode
       ,lower(p.skuname) as skuname
       , p.skucount
       --, sc2p.skucount
       , i.inactiveskus_count
       , d.donotuseskus_count
       , i.inactiveskus
       , d.donotuseskus
       , k.spicy_sku_count
       , k.spicy_skus
       --,r.created_by as author
       --,r.label
       --,round(p.cost1p,2) as cost1p
       ,p.seasonalityrisk
       ,round(p.cost2p,2) as cost2p
       --,round(p.cost3p,2) as cost3p
       ,round(p.cost4p,2) as cost4p
       ,p.pricemissingskus
     ,u.last_used as lastused
     --,u.last_used_running_week
     --,u.next_used as nextused
     --,u.next_used_running_week
     ,case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
     --,case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
     ,coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
     --,coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
     ,r.is_default as isdefault
     ,dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
     ,r.updated_at as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
from materialized_views.isa_services_recipe_consolidated as r
left join recipe_usage u on u.recipe_code = r.recipe_code
left join nutrition n on n.recipe_id = r.id
left join (select * from cost where size=1) rc_1 on rc_1.recipe_id=r.id
left join (select * from cost where size=2) rc_2 on rc_2.recipe_id=r.id
left join (select * from cost where size=3) rc_3 on rc_3.recipe_id=r.id
left join (select * from cost where size=4) rc_4 on rc_4.recipe_id=r.id
left join picklists as p on p.unique_recipe_code=r.unique_recipe_code
left join (select * from inactiveskus_INT where market = 'ie' and segment_name = 'IE' ) as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_INT where market = 'ie' and segment_name = 'IE' ) as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_INT where market = 'ie' and segment_name = 'IE') as k on p.unique_recipe_code = k.unique_recipe_code
left join steps_INT as steps ON steps.recipe_id = r.id
where lower(r.status) in ('ready for menu planning', 'in development')
    and  r.market='ie'
    and length(r.primary_protein)>0
    and r.primary_protein <>'N/A'
    and  p.cost2p >0
    and  p.cost4p >0
) temp
where isdefault=1)

select * from all_recipes_IE



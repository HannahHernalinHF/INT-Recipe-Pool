----- INT Magnet Recipe Pool Query -----

WITH scores_DKSE_NO AS (SELECT case when country IN ('DK','SE') then 'DKSE' else country end as country
                          , region
                          , mainrecipecode
                          , sum(rating_count) as rating_count
                          , SUM(score * rating_count) / SUM(rating_count)                AS scorewoscm
                          , SUM(score_wscm * rating_count_wscm) / SUM(rating_count_wscm) AS scorescm
                     FROM (SELECT *
                                , dense_rank() over (partition BY mainrecipecode, region
                                ORDER BY hellofresh_week DESC) AS o
                           FROM materialized_views.gamp_recipe_scores
                           WHERE region IN ('DK','SE','NO')
                             AND score > 0
                             AND rating_count > 50) t
                     WHERE o = 1
                     GROUP BY 1, 2, 3)

, seasonality_DKSE_NO AS (
    SELECT sku
         , MAX(seasonality_score) AS seasonality_score
    FROM uploads.gp_sku_seasonality
    WHERE country IN ('NORDICS')
      --AND week >= 'W39'
      --AND week <= 'W65'
    GROUP BY 1
)
   , recipe_usage_DKSE_NO AS (
    SELECT *
         , CASE
               WHEN last_used_running_week IS NOT NULL AND next_used_running_week IS NOT NULL
                   THEN next_used_running_week - last_used_running_week
               ELSE 0 END AS lastnextuseddiff
    FROM materialized_views.isa_services_recipe_usage r
    WHERE --r.market = 'dkse' AND
          r.region_code in ('se','no', 'it')
)
   , nutrition_INT AS (
    SELECT *
    FROM materialized_views.culinary_services_recipe_segment_nutrition
    WHERE market IN ('dkse', 'it', 'jp')
      AND segment IN ('SE','NO', 'IT', 'JP')
)
   , cost_INT AS (
    SELECT segment
         , recipe_id
         , size
         , AVG (price) AS cost
    FROM materialized_views.culinary_services_recipe_static_price
    WHERE segment IN ('SE','NO','IT','JP','GR')
      --AND distribution_center IN ('SK','MO')
      --AND hellofresh_week >= '2022-W37'
      --AND hellofresh_week <= '2022-W65'
    GROUP BY 1, 2, 3
    )
    , sku_cost_DKSE_NO AS (
        SELECT code
                , AVG (price) AS price
        FROM materialized_views.procurement_services_staticprices sp
            LEFT JOIN materialized_views.procurement_services_culinarysku sku
        ON sku.id=sp.culinary_sku_id
        WHERE sku.market='dkse'
          AND sp.distribution_center IN ('SK','MO')
          --AND sp.hellofresh_week >= '2022-W37'
          --AND sp.hellofresh_week <= '2022-W65'
        GROUP BY 1
    )
    , picklists_DKSE_NO AS (
        SELECT segment_name
                , unique_recipe_code
                , group_concat(code, " | ") AS skucode
                , group_concat(NAME, " | ") AS skuname
                , MAX (COALESCE (seasonality_score, 0)) AS seasonalityrisk
                --, SUM (COALESCE (boxitem, 0)) AS boxitem
                , COUNT (DISTINCT code) AS skucount
                , SUM (cost1p) AS cost1p
                , SUM (cost2p) AS cost2p
                , SUM (cost3p) AS cost3p
                , SUM (cost4p) AS cost4p
                , group_concat(price_missing, " | ") AS pricemissingskus
                , group_concat(status, " | ") as sku_status
                , size
        FROM (
            SELECT p.segment_name
                , r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , seasonality_score
                --, boxitem
                , CASE WHEN price IS NULL OR price=0 THEN p.code ELSE NULL END AS price_missing
                , SUM (CASE WHEN SIZE = 1 THEN pick_count * price ELSE 0 END) AS cost1p
                , SUM (CASE WHEN SIZE = 2 THEN pick_count * price ELSE 0 END) AS cost2p
                , SUM (CASE WHEN SIZE = 3 THEN pick_count * price ELSE 0 END) AS cost3p
                , SUM (CASE WHEN SIZE = 4 THEN pick_count * price ELSE 0 END) AS cost4p
                , skus.status
                , p.size
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
               ON skus.id = p.culinarysku_id
            LEFT JOIN sku_cost_DKSE_NO C
            ON C.code = p.code
            LEFT JOIN seasonality_DKSE_NO s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name IN ('SE','NO')
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12 ) t
        GROUP BY 1,2,13
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
            WHERE r.market IN ('dkse', 'it','jp')
            AND p.segment_name IN ('SE', 'NO', 'IT','JP')
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
            WHERE r.market IN ('dkse', 'it','jp')
            AND p.segment_name IN ('SE', 'NO', 'IT','JP')
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
            WHERE r.market IN ('dkse', 'it','jp')
            AND p.segment_name IN ('SE', 'NO', 'IT','JP')
            AND lower(p.name) LIKE '%chili%'
                 OR lower(p.name) LIKE '%chilli%'
                 OR lower(p.name) LIKE '%sriracha%'
                 OR lower(p.name) LIKE '%jalapeno%'
                 OR lower(p.name) LIKE '%chorizo sausage%'
                 OR lower(p.name) LIKE '%wasabi%'
                 OR lower(p.name) LIKE '%karashi%'
                 AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 7--, 11, 12
         ) t
    GROUP BY 1,2,3    )

, filtered_recipes_DKSE as (
    select *
    from (
         select r.*
                , upper(r.market) as country
                , round(p.cost1p,2) as cost1p
                , round(p.cost2p,2) as cost2p
                , round(p.cost3p,2) as cost3p
                , round(p.cost4p,2) as cost4p
                , p.pricemissingskus
                , p.skucode
                , p.skuname
                , p.skucount
                , p.seasonalityrisk
                --, p.boxitem
                , dense_rank() over (partition by r.recipe_code, r.market
                                        order by r.version  desc) as o
        from materialized_views.isa_services_recipe_consolidated as r
            left join (select * from picklists_DKSE_NO where segment_name = 'SE') as p
                on p.unique_recipe_code=r.unique_recipe_code
        where   r.market = 'dkse'
                and length (r.primary_protein)>0
                and r.primary_protein <>'N/A'
                and p.cost2p > 0
    ) temp
    where temp.o = 1
)

, filtered_recipes_NO as (
    select *
    from (
         select r.*
                , 'NO' as country
                , round(p.cost1p,2) as cost1p
                , round(p.cost2p,2) as cost2p
                , round(p.cost3p,2) as cost3p
                , round(p.cost4p,2) as cost4p
                , p.pricemissingskus
                , p.skucode
                , p.skuname
                , p.skucount
                , p.seasonalityrisk
                --, p.boxitem
                , dense_rank() over (partition by r.recipe_code, r.market
                                        order by r.version  desc) as o
        from materialized_views.isa_services_recipe_consolidated as r
            left join (select * from picklists_DKSE_NO where segment_name = 'NO') as p
                on p.unique_recipe_code=r.unique_recipe_code
        where   r.market = 'dkse'
                and length (r.primary_protein)>0
                and r.primary_protein <>'N/A'
                and p.cost2p > 0
    ) temp
    where temp.o = 1
)

, steps_INT as (
    SELECT r.id,
           steps.recipe_id,
           group_concat(steps.title, " | ") AS step_title,
           group_concat(steps.description," | ") as step_description
    FROM materialized_views.culinary_services_recipe_steps_translations as steps
    JOIN materialized_views.isa_services_recipe_consolidated as r
        ON r.id = steps.recipe_id
    GROUP BY 1,2
    )

, all_recipes_DKSE as(
    select
             r.id as uuid
            , r.country
            , r.unique_recipe_code as uniquerecipecode
            , r.recipe_code as code
            , r.version
            , r.status
            , regexp_replace(r.title, '\t|\n', '') as title
            , concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
            , case when r.primary_protein IS NULL OR r.primary_protein = '' then 'not available' else r.primary_protein end as primaryprotein
            , r.main_protein as mainprotein
            , r.protein_cut as proteincut
            , coalesce(r.secondary_protein,'none') as secondaryprotein
            , r.proteins
            , case when r.primary_starch IS NULL OR r.primary_starch = '' then 'not available' else r.primary_starch end as primarystarch
            , r.main_starch as mainstarch
            , coalesce(r.secondary_starch,'none') as secondarystarch
            , r.starches
            , CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' then 'not available' else coalesce(r.primary_vegetable,'none') end as primaryvegetable
            , r.main_vegetable as mainvegetable
            --, r.vegetables
            , coalesce(r.secondary_vegetable,'none') as secondaryvegetable
            --, coalesce(r.tertiary_vegetable,'none') as tertiaryvegetable
            --, coalesce(r.primary_dry_spice,'none') as primarydryspice
            , coalesce(r.primary_cheese,'none') as primarycheese
            , coalesce(r.primary_fruit,'none') as primaryfruit
            , coalesce(r.primary_dairy,'none') as primarydairy
            --, coalesce(r.primary_fresh_herb,'none') as primaryfreshherb
            --, coalesce(r.sauce_paste,'none') as saucepaste
            , case when n.salt is null then 0 else n.salt end as salt
            , case when n.energy = 0 or n.energy is null then 999 else n.energy end as calories
            , case when n.carbs = 0 or n.carbs is null then 999 else n.carbs end as carbohydrates
            , case when n.proteins = 0 or n.proteins is null then 999 else n.proteins end as n_proteins
            , case when r.cuisine IS NULL OR r.cuisine = '' then 'not available' else r.cuisine end as cuisine
            , case when r.dish_type IS NULL OR r.dish_type = '' then 'not available' else r.dish_type end as dishtype
            , case when r.hands_on_time ='' or r.hands_on_time is NULL then cast(99 as float)
                else cast(r.hands_on_time as float) end as handsontime
            , case when r.hands_on_time ='' or r.hands_on_time is NULL then cast(99 as float)
                 else cast(r.hands_on_time as float) end
                  +
              case when r.hands_off_time ='' or r.hands_off_time is NULL then cast(99 as float)
                 else cast(r.hands_off_time as float) end
                  as totaltime
            , r.difficulty
            --, r.tags as tag
            , case when r.target_preferences IS NULL OR r.target_preferences = '' then 'not available' else r.target_preferences end as preference
            , concat (r.tags,r.target_preferences) as preftag
            --, r.target_products as producttype
            , r.recipe_type as recipetype
            --, r.created_by as author
            --, r.label
            , r.skucode
            ,lower(r.skuname) as skuname
            , p.skucount
            --, sc2p.skucount
            , i.inactiveskus_count
            , d.donotuseskus_count
            , i.inactiveskus
            , d.donotuseskus
            , k.spicy_sku_count
            , k.spicy_skus
            , r.seasonalityrisk
            --, r.cost1p
            , r.cost2p
            --, r.cost3p
            , r.cost4p
            , r.pricemissingskus
            --, r.boxitem
            , u.last_used as lastused
            --, u.last_used_running_week
            --, u.next_used as nextused
            --, u.next_used_running_week
            , case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
            --, case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
            --, u.lastnextuseddiff
            , coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
            --, coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
            , r.is_default as isdefault
            , r.o
            , r.updated_at as updated_at
            , case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            , case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
    from filtered_recipes_DKSE as r
        left join (select * from recipe_usage_DKSE_NO where region_code = 'se') as u
            on u.recipe_code = r.recipe_code
        left join (select * from nutrition_INT where market = 'dkse' AND segment = 'SE') as n
            on n.recipe_id = r.id
        left join (select * from cost_INT where size=1 and segment = 'SE') rc_1
            on rc_1.recipe_id=r.id
        left join (select * from cost_INT where size=2 and segment = 'SE') rc_2
            on rc_2.recipe_id=r.id
        left join (select * from cost_INT where size=3 and segment = 'SE') rc_3
            on rc_3.recipe_id=r.id
        left join (select * from cost_INT where size=4 and segment = 'SE') rc_4
            on rc_4.recipe_id=r.id
        left join (select * from picklists_DKSE_NO where segment_name = 'SE') p
                on p.unique_recipe_code=r.unique_recipe_code
        left join (select * from inactiveskus_INT where market = 'dkse' and segment_name = 'SE' ) as i
                on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
        left join (select * from donotuseskus_INT where market = 'dkse' and segment_name = 'SE' ) as d
                on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
        left join (select * from spicysku_INT where market = 'dkse' and segment_name = 'SE') as k
                on p.unique_recipe_code = k.unique_recipe_code
        left join steps_INT as steps ON steps.recipe_id = r.id
    where lower(r.status) not in ('inactive','rejected', 'on hold')
)

, all_recipes_NO as(
    select
             r.id as uuid
            , r.country
            , r.unique_recipe_code as uniquerecipecode
            , r.recipe_code as code
            , r.version
            , r.status
            , regexp_replace(r.title, '\t|\n', '') as title
            , concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
            , case when r.primary_protein IS NULL OR r.primary_protein = '' then 'not available' else r.primary_protein end as primaryprotein
            , r.main_protein as mainprotein
            , r.protein_cut as proteincut
            , coalesce(r.secondary_protein,'none') as secondaryprotein
            , r.proteins
            , case when r.primary_starch IS NULL OR r.primary_starch = '' then 'not available' else r.primary_starch end as primarystarch
            , r.main_starch as mainstarch
            , coalesce(r.secondary_starch,'none') as secondarystarch
            , r.starches
            , CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' then 'not available' else coalesce(r.primary_vegetable,'none') end as primaryvegetable
            , r.main_vegetable as mainvegetable
            --, r.vegetables
            , coalesce(r.secondary_vegetable,'none') as secondaryvegetable
            --, coalesce(r.tertiary_vegetable,'none') as tertiaryvegetable
            --, coalesce(r.primary_dry_spice,'none') as primarydryspice
            , coalesce(r.primary_cheese,'none') as primarycheese
            , coalesce(r.primary_fruit,'none') as primaryfruit
            , coalesce(r.primary_dairy,'none') as primarydairy
            --, coalesce(r.primary_fresh_herb,'none') as primaryfreshherb
            --, coalesce(r.sauce_paste,'none') as saucepaste
            , case when n.salt is null then 0 else n.salt end as salt
            , case when n.energy = 0 or n.energy is null then 999 else n.energy end as calories
            , case when n.carbs = 0 or n.carbs is null then 999 else n.carbs end as carbohydrates
            , case when n.proteins = 0 or n.proteins is null then 999 else n.proteins end as n_proteins
            , case when r.cuisine IS NULL OR r.cuisine = '' then 'not available' else r.cuisine end as cuisine
            , case when r.dish_type IS NULL OR r.dish_type = '' then 'not available' else r.dish_type end as dishtype
            , case when r.hands_on_time ='' or r.hands_on_time is NULL then cast(99 as float)
                else cast(r.hands_on_time as float) end as handsontime
            , case when r.hands_on_time ='' or r.hands_on_time is NULL then cast(99 as float)
                 else cast(r.hands_on_time as float) end
                  +
              case when r.hands_off_time ='' or r.hands_off_time is NULL then cast(99 as float)
                 else cast(r.hands_off_time as float) end
                  as totaltime
            , r.difficulty
            --, r.tags as tag
            , case when r.target_preferences IS NULL OR r.target_preferences = '' then 'not available' else r.target_preferences end as preference
            , concat (r.tags,r.target_preferences) as preftag
            --, r.target_products as producttype
            , r.recipe_type as recipetype
            --, r.created_by as author
            --, r.label
            , r.skucode
            ,lower(r.skuname) as skuname
            , p.skucount
            --, sc2p.skucount
            , i.inactiveskus_count
            , d.donotuseskus_count
            , i.inactiveskus
            , d.donotuseskus
            , k.spicy_sku_count
            , k.spicy_skus
            , r.seasonalityrisk
            --, r.cost1p
            , r.cost2p
            --, r.cost3p
            , r.cost4p
            , r.pricemissingskus
            --, r.boxitem
            , u.last_used as lastused
            --, u.last_used_running_week
            --, u.next_used as nextused
            --, u.next_used_running_week
            , case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
            --, case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
            --, u.lastnextuseddiff
            , coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
            --, coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
            , r.is_default as isdefault
            , r.o
            , r.updated_at as updated_at
            , case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            , case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
    from filtered_recipes_NO as r
        left join (select * from recipe_usage_DKSE_NO where region_code = 'no') as u
            on u.recipe_code = r.recipe_code
        left join (select * from nutrition_INT where market = 'dkse' AND segment = 'NO') as n
            on n.recipe_id = r.id
        left join (select * from cost_INT where size=1 and segment = 'NO') rc_1
            on rc_1.recipe_id=r.id
        left join (select * from cost_INT where size=2 and segment = 'NO') rc_2
            on rc_2.recipe_id=r.id
        left join (select * from cost_INT where size=3 and segment = 'NO') rc_3
            on rc_3.recipe_id=r.id
        left join (select * from cost_INT where size=4 and segment = 'NO') rc_4
            on rc_4.recipe_id=r.id
        left join (select * from picklists_DKSE_NO where segment_name = 'NO') p
                on p.unique_recipe_code=r.unique_recipe_code
        left join (select * from inactiveskus_INT where market = 'dkse' and segment_name = 'NO' ) as i
                on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
        left join (select * from donotuseskus_INT where market = 'dkse' and segment_name = 'NO' ) as d
                on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
        left join (select * from spicysku_INT where market = 'dkse' and segment_name = 'NO') as k
                on p.unique_recipe_code = k.unique_recipe_code
        left join steps_INT as steps ON steps.recipe_id = r.id
    where lower(r.status) not in ('inactive','rejected', 'on hold')
)

,seasonality_IT_JP_GB as(
select country,
    sku,
    max(seasonality_score) as seasonality_score
from uploads.gp_sku_seasonality
where country IN ('IT','JP','GB') --and week>='W37'and week<='W65'
group by 1,2
)

, recipe_usage_IT_JP as(
select * from materialized_views.isa_services_recipe_usage
where region_code in ('it','jp') and market in ('it','jp')
)

, recipe_usage_GB as(
select * from materialized_views.isa_services_recipe_usage
where market = 'gb'
)

, sku_cost_IT_JP_GB as(
select market, code,  avg(price) as price
from materialized_views.procurement_services_staticprices sp
left join materialized_views.procurement_services_culinarysku sku
on sku.id=sp.culinary_sku_id
where  sku.market in ('it','jp','gb') --and sp.hellofresh_week >= '2022-W37' and sp.hellofresh_week <= '2022-W65'
group by 1,2
)

,picklists_IT as(
    select market
    , segment_name
    , unique_recipe_code
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
    , size
    from (
        select r.market
        , p.segment_name
        , r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') as name
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        , pk.status
        , p.size
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code
        left join (select * from sku_cost_IT_JP_GB where market = 'it') as c on c.code = p.code
        left join (select * from seasonality_IT_JP_GB where country = 'IT') as s on s.sku = p.code
        where r.market in ('it') and p.segment_name IN ('IT') and p.size = 2
        group by 1,2,3,4,5,6,7,8,13,14) t
    group by 1,2,3,14
)

,picklists_JP as(
    select market
    , segment_name
    , unique_recipe_code
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
    , size
    from (
        select r.market
        , p.segment_name
        , r.unique_recipe_code
        , p.code
        , regexp_replace(p.name, '\t|\n', '') as name
        , pk.packaging_type
        , seasonality_score
        , case when price is null or price=0 then p.code else NULL end as price_missing
        , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
        , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
        , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
        , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
        , pk.status
        , p.size
        from materialized_views.isa_services_recipe_consolidated r
        join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p on r.id = p.recipe_id
        join materialized_views.procurement_services_culinarysku pk on p.code = pk.code
        left join (select * from sku_cost_IT_JP_GB where market = 'jp') as c on c.code = p.code
        left join (select * from seasonality_IT_JP_GB where country = 'JP') as s on s.sku = p.code
        where r.market in ('jp') and p.segment_name IN ('JP') and p.size = 2
        group by 1,2,3,4,5,6,7,8,13,14) t
    group by 1,2,3,14
)


, all_recipes_IT as(
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
left join (select * from recipe_usage_IT_JP where region_code = 'it' and market = 'it') as u on u.recipe_code = r.recipe_code
left join (select * from nutrition_INT where market = 'it' AND segment = 'IT') n on n.recipe_id = r.id
left join (select * from cost_INT where size=1 and segment = 'IT') rc_1 on rc_1.recipe_id=r.id
left join (select * from cost_INT where size=2 and segment = 'IT') rc_2 on rc_2.recipe_id=r.id
left join (select * from cost_INT where size=3 and segment = 'IT') rc_3 on rc_3.recipe_id=r.id
left join (select * from cost_INT where size=4 and segment = 'IT') rc_4 on rc_4.recipe_id=r.id
left join (select * from picklists_IT where market = 'it' and segment_name = 'IT') as p on p.unique_recipe_code=r.unique_recipe_code
left join (select * from inactiveskus_INT where market = 'it' and segment_name = 'IT' ) as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_INT where market = 'it' and segment_name = 'IT' ) as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_INT where market = 'it' and segment_name = 'IT') as k on p.unique_recipe_code = k.unique_recipe_code
left join steps_INT as steps ON steps.recipe_id = r.id
where lower(r.status) in ('ready for menu planning', 'in development')
    and  r.market='it'
    and lower(r.recipe_type) not in ('modularity', 'add-ons')
    and lower(title) NOT LIKE '%test%'
    and lower(title) NOT LIKE '%pck%'
    --and title NOT LIKE '%Aperitivo Extra%'
) temp
where isdefault = 1 )

, all_recipes_JP as(
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
left join (select * from recipe_usage_IT_JP where region_code = 'jp' and market = 'jp')as u on u.recipe_code = r.recipe_code
left join (select * from nutrition_INT where market = 'jp' AND segment = 'JP') n on n.recipe_id = r.id
left join (select * from cost_INT where size=1 and segment = 'JP') rc_1 on rc_1.recipe_id=r.id
left join (select * from cost_INT where size=2 and segment = 'JP') rc_2 on rc_2.recipe_id=r.id
left join (select * from cost_INT where size=3 and segment = 'JP') rc_3 on rc_3.recipe_id=r.id
left join (select * from cost_INT where size=4 and segment = 'JP') rc_4 on rc_4.recipe_id=r.id
left join (select * from picklists_JP where market = 'jp' and segment_name = 'JP') as p on p.unique_recipe_code=r.unique_recipe_code
left join (select * from inactiveskus_INT where market = 'jp' and segment_name = 'JP' ) as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_INT where market = 'jp' and segment_name = 'JP' ) as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_INT where market = 'jp' and segment_name = 'JP') as k on p.unique_recipe_code = k.unique_recipe_code
left join steps_INT as steps ON steps.recipe_id = r.id
where lower(r.status) in ('ready for menu planning', 'in development')
    and r.market='jp'
-- and  p.cost2p >0
-- and  p.cost4p >0
) temp
where o=1 )

, nutrition_GB as(
select *
from materialized_views.culinary_services_recipe_segment_nutrition
where country = 'GB' and market = 'gb'
)

select distinct * from all_recipes_DKSE
UNION ALL
select distinct * from all_recipes_NO
UNION ALL
select distinct * from all_recipes_IT
UNION ALL
select distinct * from all_recipes_JP
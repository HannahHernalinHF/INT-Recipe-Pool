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
      AND week >= 'W47'
      AND week <= 'W52'
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
    WHERE market IN ('dkse', 'it', 'jp','ie')
      AND segment IN ('SE','NO', 'IT', 'JP', 'IE')
)
   , cost_INT AS (
    SELECT segment
         , recipe_id
         , size
         , AVG (price) AS cost
    FROM materialized_views.culinary_services_recipe_static_price
    WHERE segment IN ('SE','NO','IT','JP','GR','IE')
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
           r.unique_recipe_code,
           group_concat(steps.title, " | ") AS step_title,
           group_concat(steps.description," | ") as step_description
    FROM materialized_views.culinary_services_recipe_steps_translations as steps
    JOIN materialized_views.isa_services_recipe_consolidated as r
        ON r.id = steps.recipe_id
    GROUP BY 1,2,3
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
            --, r.cost4p
            --, r.pricemissingskus
            --, r.boxitem
            , u.last_used as lastused
            --, u.last_used_running_week
            --, u.next_used as nextused
            --, u.next_used_running_week
            , case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
            --, case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
            --, u.lastnextuseddiff
            , coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
            , coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
            , r.is_default as isdefault
            , r.o
            , r.updated_at as updated_at
            , case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            , case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
            , r.image_url
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
            --, r.cost4p
            --, r.pricemissingskus
            --, r.boxitem
            , u.last_used as lastused
            --, u.last_used_running_week
            --, u.next_used as nextused
            --, u.next_used_running_week
            , case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
            --, case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
            --, u.lastnextuseddiff
            , coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
            , coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
            , r.is_default as isdefault
            , r.o
            , r.updated_at as updated_at
            , case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            , case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
            , r.image_url
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

,seasonality_INT2 as(
select country,
    sku,
    max(seasonality_score) as seasonality_score
from uploads.gp_sku_seasonality
where country IN ('IT','JP','GB','IE','CA','FR','DACH') and week>='W47'and week<='W52'
group by 1,2
)

, recipe_usage_IT_JP_IE as(
select * from materialized_views.isa_services_recipe_usage
where region_code in ('it','jp','ie') and market in ('it','jp','ie')
)

, recipe_usage_GB as(
select * from materialized_views.isa_services_recipe_usage
where market = 'gb'
)

, sku_cost_IT_JP_GB_IE as(
select market, code,  avg(price) as price
from materialized_views.procurement_services_staticprices sp
left join materialized_views.procurement_services_culinarysku sku
on sku.id=sp.culinary_sku_id
where  sku.market in ('it','jp','gb','ie') --and sp.hellofresh_week >= '2022-W37' and sp.hellofresh_week <= '2022-W65'
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
        left join (select * from sku_cost_IT_JP_GB_IE where market = 'it') as c on c.code = p.code
        left join (select * from seasonality_INT2 where country = 'IT') as s on s.sku = p.code
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
        left join (select * from sku_cost_IT_JP_GB_IE where market = 'jp') as c on c.code = p.code
        left join (select * from seasonality_INT2 where country = 'JP') as s on s.sku = p.code
        where r.market in ('jp') and p.segment_name IN ('JP') and p.size = 2
        group by 1,2,3,4,5,6,7,8,13,14) t
    group by 1,2,3,14
)

,picklists_IE as(
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
        left join (select * from sku_cost_IT_JP_GB_IE where market = 'ie') as c on c.code = p.code
        left join (select * from seasonality_INT2 where country = 'IE') as s on s.sku = p.code
        where r.market = 'ie' and p.segment_name = 'IE' and p.size = 2
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
       --,round(p.cost4p,2) as cost4p
       --,p.pricemissingskus
     ,u.last_used as lastused
     --,u.last_used_running_week
     --,u.next_used as nextused
     --,u.next_used_running_week
     ,case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
     --,case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
     ,coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
     ,coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
     ,r.is_default as isdefault
     ,dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
     ,r.updated_at as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     ,r.image_url
from materialized_views.isa_services_recipe_consolidated as r
left join (select * from recipe_usage_IT_JP_IE where region_code = 'it' and market = 'it') as u on u.recipe_code = r.recipe_code
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
       --,round(p.cost4p,2) as cost4p
       --,p.pricemissingskus
     ,u.last_used as lastused
     --,u.last_used_running_week
     --,u.next_used as nextused
     --,u.next_used_running_week
     ,case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
     --,case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
     ,coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
     ,coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
     ,r.is_default as isdefault
     ,dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
     ,r.updated_at as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     ,r.image_url
from materialized_views.isa_services_recipe_consolidated as r
left join (select * from recipe_usage_IT_JP_IE where region_code = 'jp' and market = 'jp')as u on u.recipe_code = r.recipe_code
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
       --,round(p.cost4p,2) as cost4p
       --,p.pricemissingskus
     ,u.last_used as lastused
     --,u.last_used_running_week
     --,u.next_used as nextused
     --,u.next_used_running_week
     ,case when u.absolute_last_used is NULL then '' else u.absolute_last_used end as absolutelastused
     --,case when u.absolute_last_used_running_week is NULL then -1 else u.absolute_last_used_running_week end as absolutelastusedrunning
     ,coalesce(cast(u.is_newrecipe as integer),1) as isnewrecipe
     ,coalesce(cast(u.is_newscheduled as integer),0) as isnewscheduled
     ,r.is_default as isdefault
     ,dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
     ,r.updated_at as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     ,r.image_url
from materialized_views.isa_services_recipe_consolidated as r
left join (select * from recipe_usage_IT_JP_IE where region_code = 'ie' and market = 'ie') as u on u.recipe_code = r.recipe_code
left join (select * from nutrition_INT where market = 'ie' AND segment = 'IE') as n on n.recipe_id = r.id
left join (select * from cost_INT where size=1 and segment = 'IE') rc_1 on rc_1.recipe_id=r.id
left join (select * from cost_INT where size=2 and segment = 'IE') rc_2 on rc_2.recipe_id=r.id
left join (select * from cost_INT where size=3 and segment = 'IE') rc_3 on rc_3.recipe_id=r.id
left join (select * from cost_INT where size=4 and segment = 'IE') rc_4 on rc_4.recipe_id=r.id
left join picklists_IE as p on p.unique_recipe_code=r.unique_recipe_code
left join (select * from inactiveskus_INT where market = 'ie' and segment_name = 'IE' ) as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_INT where market = 'ie' and segment_name = 'IE' ) as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_INT where market = 'ie' and segment_name = 'IE') as k on p.unique_recipe_code = k.unique_recipe_code
left join steps_INT as steps ON steps.recipe_id = r.id
where lower(r.status) in ('ready for menu planning', 'in development')
    and  r.market='ie'
    and length(r.primary_protein)>0
    and r.primary_protein <>'N/A'
    and  p.cost2p >0
    --and  p.cost4p >0
) temp
where isdefault=1)

, nutrition_GB as(
select *
from materialized_views.culinary_services_recipe_segment_nutrition
where country = 'GB' and market = 'gb'
)

, last_recipe_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipes
where remps_instance IN ('CA','DACH')
)t where o=1
)

, last_cost_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipecost
where remps_instance IN ('CA','FR','DACH')
)t where o=1
)

, last_nutrition_remps AS (
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        FROM remps.recipe_nutritionalinfopp
        WHERE remps_instance IN ('CA','DACH')
    ) AS t
    WHERE o = 1)

, last_product_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_producttypes
where remps_instance IN ('CA','DACH')
)t where o=1
)

, last_preference_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_recipepreferences
where remps_instance IN ('CA','DACH')
)t where o=1
)

, last_preference_map_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_recipepreferences_recipes
where remps_instance IN ('CA','DACH')
)t where o=1
)

,last_ingredient_group_remps as(
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_ingredientgroup
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)

, last_recipe_sku_remps as(
SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_recipeskus
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)

,last_sku_remps as(
SELECT *
FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.sku_sku
    WHERE remps_instance IN ('CA','DACH')
    ) AS t
WHERE o = 1)

,picklists_CA as(
    select
    uniquerecipecode
    , group_concat(code," | ") as skucode
    , group_concat(display_name," | ") as skuname
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , count(distinct code) as skucount
    from (
        select
        r.unique_recipe_code as uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , seasonality_score
        from (select * from last_recipe_remps where remps_instance = 'CA') as r
        join (select * from last_ingredient_group_remps where remps_instance = 'CA') as ig
        on r.id = ig.ingredient_group__recipe
        join (select * from last_recipe_sku_remps where remps_instance = 'CA') as rs
        on ig.id = rs.recipe_sku__ingredient_group
        join (select * from last_sku_remps where remps_instance = 'CA') as sku
        on sku.id = rs.recipe_sku__sku
        left join (select * from seasonality_INT2 where country = 'CA') as s on s.sku=sku.code
        where  rs.quantity_to_order_2p>0
        group by 1,2,3,4) t
    group by 1
)

, inactiveskus_remps as (
    SELECT market,
        unique_recipe_code,
        group_concat(distinct skucode," | ") AS inactiveskuscode,
        group_concat(distinct skuname," | ") AS inactiveskus,
        count(distinct skuname) AS inactiveskus_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code AS skucode
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        , sku.status
        from last_recipe_remps r
        join last_ingredient_group_remps ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_remps rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku_remps sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality_INT2 as s on s.sku=sku.code
        where  rs.quantity_to_order_2p>0 AND sku.status LIKE '%Inactive%' OR sku.status LIKE '%Archived%'
        group by 1,2,3,4,5) t
    group by 1,2
    )

, donotuseskus_remps as (
    SELECT market,
        unique_recipe_code,
        group_concat(distinct skucode," | ") AS donotuseskuscode,
        group_concat(distinct skuname," | ") AS donotuseskus,
        count(distinct skuname) AS donotuseskus_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code AS skucode
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        , sku.status
        from last_recipe_remps r
        join last_ingredient_group_remps ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_remps rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku_remps sku
        on sku.id = rs.recipe_sku__sku
        where  rs.quantity_to_order_2p>0 AND sku.display_name LIKE '%DO NOT USE%' OR sku.display_name LIKE '%do not use%'
        group by 1,2,3,4,5) t
    group by 1,2
    )

 ,spicysku_remps as(
    select market
    , unique_recipe_code
    , group_concat(code," | ") as skucode
    , group_concat(skuname," | ") as spicy_skus
    , count(distinct skuname) as spicy_sku_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as skuname
        from last_recipe_remps r
        join last_ingredient_group_remps ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_remps rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku_remps sku
        on sku.id = rs.recipe_sku__sku
        where lower(sku.display_name) LIKE '%chili%'
                 OR lower(sku.display_name) LIKE '%chilli%'
                 OR lower(sku.display_name) LIKE '%sriracha%'
                 OR lower(sku.display_name) LIKE '%jalapeno%'
                 OR lower(sku.display_name) LIKE '%chorizo sausage%'
                 OR lower(sku.display_name) LIKE '%wasabi%'
                 OR lower(sku.display_name) LIKE '%karashi%'
        group by 1,2,3,4) t
    group by 1,2 )


, last_hqtag_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_hqtags
where remps_instance IN ('CA','DACH')
)t where o=1
)

, last_hqtag_map_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_hqtags_recipes
where remps_instance IN ('CA','DACH')
)t where o=1
)

,hqtag_remps as(
select rr.remps_instance,rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.original_name,','),'') as name
from last_recipe_remps rr
left join last_hqtag_map_remps m on rr.id= m.recipe_recipes_id
left join last_hqtag_remps rt on rt.id=m.recipetags_hqtags_id
group by 1,2
)

, preference_remps as(
select rr.remps_instance, rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name
from last_recipe_remps rr
left join last_preference_map_remps m on rr.id= m.recipe_recipes_id
left join last_preference_remps rp on rp.id=m.recipetags_recipepreferences_id
group by 1,2
)

,producttype_remps as(
select rr.remps_instance,rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name
from last_recipe_remps rr
left join last_product_remps rp on rp.id=rr.recipe__product_type
group by 1,2
)

, last_tag_map_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_tags_recipes
where remps_instance IN ('CA','DACH')
)t where o=1
)


, last_tag_remps as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_tags
where remps_instance IN ('CA','DACH')
)t where o=1
)

, tag_remps as(
select rr.remps_instance,rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','),'')as name
from last_recipe_remps rr
left join last_tag_map_remps m on rr.id= m.recipe_recipes_id
left join last_tag_remps rt on rt.id=m.recipetags_tags_id
group by 1,2
)


, all_recipes_CA as(
select * from(
select cast(r.id as varchar) as rempsid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode as code
       ,r.version
       ,r.status
       ,r.title
       ,concat(r.title,coalesce (regexp_replace(r.subtitle, '\t|\n', ''),''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,''), coalesce(r.primaryvegetable,''),coalesce(r.primaryfruit,'')) as subtitle
       ,case when r.primaryprotein IS NULL OR r.primaryprotein = '' then 'not available' else r.primaryprotein end as primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) as mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) as proteincut
       ,coalesce(r.secondaryprotein,'none') as secondaryprotein
       ,concat(coalesce (r.primaryprotein,''),coalesce(r.secondaryprotein,'none')) as proteins
       ,case when r.primarystarch IS NULL OR r.primarystarch = '' then 'not available' else r.primarystarch end as primarystarch
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
       ,case when r.handsontime ="" or r.handsontime is NULL then 0
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is NULL then 0
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is not NULL then cast( left(r.handsontime,2) as float)
             when length (r.handsontime) <2 then 0
             when r.handsontime='0' then 0
             else cast(r.handsontime as float) end as handsontime
       ,case when r.totaltime ="" or r.totaltime is NULL then 0
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is NULL then 0
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is not NULL then cast( left(r.totaltime,2) as float)
             when length (r.totaltime) <2 then 0
             when r.totaltime='0' then 0
             else cast(r.totaltime as float) end as totaltime
       ,cast(right(difficultylevel,1) as int) as difficulty
       --,ht.name as hqtag
       --,rt.name as tag
       ,case when pf.name IS NULL or pf.name = '' then 'not available' else pf.name end as preference
       ,concat (ht.name,rt.name,pf.name) as preftag
       ,pt.name as producttype
       ,p.skucode
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
       ,round(rc.cost_2p,2) as cost2p
       --,round(rc.cost_3p,2) as cost3p
       --,round(rc.cost_4p,2) as cost4p
       --,p.skucode as pricemissingskus -- (disregard as this is just a dummy)
       --,round(rc.cost_1p,2) as cost1p
      ,r.lastused
       --,r.nextused
      ,r.absolutelastused
     ,case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end as isnewrecipe
     ,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
     ,r.isdefault as isdefault
     ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
     ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     ,r.mainimageurl as image_url
from materialized_views.int_scm_analytics_remps_recipe as r
left join (select * from last_recipe_remps where remps_instance = 'CA') as r2 on r2.unique_recipe_code=r.uniquerecipecode
left join (select * from last_cost_remps where remps_instance = 'CA') as rc on rc.recipe_cost__recipe=r2.id
left join (select * from last_nutrition_remps where remps_instance = 'CA') as n on n.id=r.nutritionalinfo2p
left join picklists_CA as p on p.uniquerecipecode=r.uniquerecipecode
left join (select * from preference_remps where remps_instance = 'CA') as pf on pf.uniquerecipecode=r.uniquerecipecode
left join (select * from hqtag_remps where remps_instance = 'CA') as ht on ht.uniquerecipecode=r.uniquerecipecode
left join (select * from tag_remps where remps_instance = 'CA') as rt on rt.uniquerecipecode=r.uniquerecipecode
left join (select * from producttype_remps where remps_instance = 'CA') as pt on pt.uniquerecipecode=r.uniquerecipecode
left join (select * from inactiveskus_remps where market='CA') as i on p.uniquerecipecode = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_remps where market='CA') as d on p.uniquerecipecode = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_remps where market='CA') as k on p.uniquerecipecode = k.unique_recipe_code
left join steps_INT as steps ON steps.unique_recipe_code = r.uniquerecipecode
where lower(r.status) in ('ready for menu planning','active','in development')
    and lower(r.title) not like '%not use%' and lower(r.title) not like '%wrong%' and lower(r.title) not like '%test%'
    and r.uniquerecipecode not like 'C%'
    and r.uniquerecipecode not like '%-FR'
     and r.uniquerecipecode not like 'ADD%'
     and r.uniquerecipecode not like 'RMOD%'
     and r.uniquerecipecode not like 'RCON%'
     and r.uniquerecipecode not like 'RAO%'
     and r.uniquerecipecode not like 'RRS%'
    and r.primaryprotein is not NULL
     and length (primaryprotein)>0
    and primaryprotein <>'N/A'
    and  r.country='CA'
    and rc.cost_2p >0
    and rc.recipe_cost__distribution_centre=118219490218475521

) temp
where o=1 --and scorewoscm>0
)

,last_sku_cost_FR as(
    select code, status, avg(price) as price
    from materialized_views.procurement_services_staticprices sp
    left join materialized_views.procurement_services_culinarysku sku
    on sku.id=sp.culinary_sku_id
    where sku.market='beneluxfr' and sp.distribution_center='DH' --and  sp.hellofresh_week >= '{weekstr1}' and sp.hellofresh_week <= '{weekstr4}'
    group by 1,2
)

, last_recipe_FR as(
    select *
    from materialized_views.remps_recipe
    where remps_instance = 'FR'
)

, last_nutrition_FR AS (
    select *
    from materialized_views.remps_recipe_nutritionalinfopp
    where remps_instance = 'FR'
)
, last_tag_FR as(
    select *
    from materialized_views.remps_recipetags_tags
    where remps_instance = 'FR'
)

, last_tag_map_FR as(
    select *
    from materialized_views.remps_map_tags_recipes
    where remps_instance='FR'
)

, last_product_FR as(
    select *
    from materialized_views.remps_recipe_producttypes
    where remps_instance = 'FR'
)

, last_preference_FR as(
    select *
    from materialized_views.remps_recipetags_recipepreferences
    where remps_instance = 'FR'
)

, last_preference_map_FR as(
    select *
    from materialized_views.remps_map_recipepreferences_recipes
    where remps_instance = 'FR'
)

, last_hqtag_FR as(
    select *
    from materialized_views.remps_recipetags_hqtags
    where remps_instance = 'FR'
)

, last_hqtag_map_FR as(
    select *
    from materialized_views.remps_map_hqtags_recipes
    where remps_instance = 'FR'
)

,last_ingredient_group_FR as(
    select *
    from materialized_views.remps_recipe_ingredientgroup
    where remps_instance = 'FR'
)

,last_recipe_sku_FR as(
    select *
    from materialized_views.remps_recipe_recipeskus
    where remps_instance = 'FR'
)

,last_sku_FR as(
    select *
    from materialized_views.remps_sku_sku
    where remps_instance = 'FR'
)

,hqtag_FR as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.original_name,','),'') as name
from last_recipe_FR rr
left join last_hqtag_map_FR m on rr.id= m.recipe_recipes_id
left join last_hqtag_FR rt on rt.id=m.recipetags_hqtags_id
group by 1
)

, preference_FR as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name
from last_recipe_FR rr
left join last_preference_map_FR m on rr.id= m.recipe_recipes_id
left join last_preference_FR rp on rp.id=m.recipetags_recipepreferences_id
group by 1
)

,producttype_FR as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name
from last_recipe_FR rr
left join last_product_FR rp on rp.id=rr.recipe__product_type
group by 1
)

, tag_FR as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','),'')as name
from last_recipe_FR rr
left join last_tag_map_FR m on rr.id= m.recipe_recipes_id
left join last_tag_FR rt on rt.id=m.recipetags_tags_id
group by 1
)

,weeks_FR as(
select distinct hellofresh_week, hellofresh_running_week
from dimensions.date_dimension
)

,picklists_FR as(
    select market
    , unique_recipe_code
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
    , status
    , size
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , seasonality_score
        , sc.price
        , case when sc.price is null or sc.price=0 then regexp_replace(sku.code, '\t|\n', '') else NULL end as price_missing
        , case when sc.price is null or sc.price=0 then regexp_replace(sku.name, '\t|\n', '') else NULL end as price_missing_name
        , rs.quantity_to_order_1p
        , rs.quantity_to_order_2p
        , rs.quantity_to_order_3p
        , rs.quantity_to_order_4p
        , sku.status
        , rp.size
        from last_recipe_FR r
        join last_ingredient_group_FR ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_FR rs
        on ig.id = rs.recipe_sku__ingredient_group
        left join materialized_views.remps_marketsetup_distributioncentres dc
        on rs.recipe_sku__distribution_centre = dc.id
        join last_sku_FR sku
        on sku.id = rs.recipe_sku__sku
        left join (select * from seasonality_INT2 where country = 'FR') as s
        on s.sku=sku.code
        left join last_sku_cost_FR sc
        on sc.code=sku.code
        left join materialized_views.remps_picklists as rp
        on rp.sku_code = sku.code
        where  (rs.quantity_to_order_2p>0 or rs.quantity_to_order_3p>0 or rs.quantity_to_order_4p>0)
        and r.remps_instance = 'FR' and dc.bob_code = 'DH' and rp.size = 2
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14) t
    group by 1,2,13,14
)

,inactiveskus_FR as(
    select market
    , unique_recipe_code
    , group_concat(code," | ") as inactiveskuscodes
    , group_concat(display_name," | ") as inactiveskus
    , count(distinct code) as inactiveskus_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , sku.status
        , rp.size
        from last_recipe_FR r
        join last_ingredient_group_FR ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_FR rs
        on ig.id = rs.recipe_sku__ingredient_group
        left join materialized_views.remps_marketsetup_distributioncentres dc
        on rs.recipe_sku__distribution_centre = dc.id
        join last_sku_FR sku
        on sku.id = rs.recipe_sku__sku
        left join materialized_views.remps_picklists as rp
        on rp.sku_code = sku.code
        where  (rs.quantity_to_order_2p>0 or rs.quantity_to_order_3p>0 or rs.quantity_to_order_4p>0)
        and r.remps_instance = 'FR' and dc.bob_code = 'DH' and rp.size = 2
        group by 1,2,3,4,5,6) t
    where status like '%Inactive%' OR status LIKE '%Archived%'
    group by 1,2
)

,donotuseskus_FR as(
    select market
    , unique_recipe_code
    , group_concat(code," | ") as donotuseskuscodes
    , group_concat(display_name," | ") as donotuseskus
    , count(distinct code) as donotuseskus_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , sku.status
        , rp.size
        from last_recipe_FR r
        join last_ingredient_group_FR ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_FR rs
        on ig.id = rs.recipe_sku__ingredient_group
        left join materialized_views.remps_marketsetup_distributioncentres dc
        on rs.recipe_sku__distribution_centre = dc.id
        join last_sku_FR sku
        on sku.id = rs.recipe_sku__sku
        left join materialized_views.remps_picklists as rp
        on rp.sku_code = sku.code
        where  (rs.quantity_to_order_2p>0 or rs.quantity_to_order_3p>0 or rs.quantity_to_order_4p>0)
        and r.remps_instance = 'FR' and dc.bob_code = 'DH' and rp.size = 2
        and sku.display_name LIKE '%DO NOT USE%' OR sku.display_name LIKE '%do not use%'
        group by 1,2,3,4,5,6) t
    group by 1,2
)

,spicysku_FR as(
    select market
    , unique_recipe_code
    , group_concat(display_name," | ") as spicy_skus
    , count(distinct display_name) as spicy_sku_count
    from (
        select r.remps_instance as market
        , r.unique_recipe_code
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , sku.status
        , rp.size
        from last_recipe_FR r
        join last_ingredient_group_FR ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_FR rs
        on ig.id = rs.recipe_sku__ingredient_group
        left join materialized_views.remps_marketsetup_distributioncentres dc
        on rs.recipe_sku__distribution_centre = dc.id
        join last_sku_FR sku
        on sku.id = rs.recipe_sku__sku
        left join materialized_views.remps_picklists as rp
        on rp.sku_code = sku.code
        where  (rs.quantity_to_order_2p>0 or rs.quantity_to_order_3p>0 or rs.quantity_to_order_4p>0)
        and r.remps_instance = 'FR' and dc.bob_code = 'DH' and rp.size = 2
        and lower(sku.display_name) LIKE '%chili%'
                 OR lower(sku.display_name) LIKE '%chilli%'
                 OR lower(sku.display_name) LIKE '%sriracha%'
                 OR lower(sku.display_name) LIKE '%jalapeno%'
                 OR lower(sku.display_name) LIKE '%chorizo sausage%'
                 OR lower(sku.display_name) LIKE '%wasabi%'
                 OR lower(sku.display_name) LIKE '%karashi%'
        group by 1,2,3,4,5,6) t
    group by 1,2
)

, all_recipes_FR as(
select * from(
select cast(r.id as varchar) as rempsid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode as code
       ,r.version
       ,r.status
       ,r.title
       ,concat(r.title,coalesce (r.subtitle,''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,''), coalesce(r.primaryvegetable,'')) as subtitle
       ,r.primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) as mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) as proteincut
       ,coalesce(r.secondaryprotein,'none') as secondaryprotein
       ,concat(coalesce (r.primaryprotein,''),coalesce(r.secondaryprotein,'none')) as proteins
       ,r.primarystarch
       ,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') as mainstarch
       ,coalesce(r.secondarystarch,'none') as secondarystarch
       ,concat(coalesce (r.primarystarch,''),coalesce(r.secondarystarch,'none')) as starches
       ,coalesce(r.primaryvegetable,'none') as primaryvegetable
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
       ,case when n.carbohydrates=0 then 999 else n.carbohydrates end as carbohydrates
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
       ,cast(right(difficultylevel,1) as int) as difficulty
       --,ht.name as hqtag
       --,rt.name as tag
       ,case when pf.name IS NULL or pf.name = '' then 'not available' else pf.name end as preference
       ,concat (ht.name,rt.name,pf.name) as preftag
       ,pt.name as producttype
       --,r.author
       ,p.skucode
       ,p.skuname
       ,p.skucount
       ,i.inactiveskus_count
       ,d.donotuseskus_count
       ,i.inactiveskus
       ,d.donotuseskus
       ,k.spicy_sku_count
       ,k.spicy_skus
       ,p.seasonalityrisk
       --,round(p.cost_1p,2) as cost1p
       ,round(p.cost_2p,2) as cost2p
       --,round(p.cost_3p,2) as cost3p
       --,round(p.cost_4p,2) as cost4p
       --,coalesce(u.budget,0) as budget
       --,coalesce(round(u.budget-round(rc.cost_2p,2),2),-round(rc.cost_2p,2)) as differencebudgetcost2p
       --,p.pricemissingskus
       --,p.pricemissingskunames
       ,r.lastused
       --,r.nextused
       ,r.absolutelastused
       --,case when w.hellofresh_running_week is NOT NULL then w.hellofresh_running_week else -1 end as absolutelastusedrunning
       ,case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end as isnewrecipe
       ,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
       ,r.isdefault as isdefault
       ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
       ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
       ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
       ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
       ,r.mainimageurl as image_url
from (select* from materialized_views.int_scm_analytics_remps_recipe where country = 'FR') as r
left join last_recipe_FR r2 on r2.unique_recipe_code=r.uniquerecipecode
left join (select * from last_cost_remps where remps_instance = 'FR') as rc on rc.id=r2.recipe__recipe_cost
left join last_nutrition_FR n on n.id=r.nutritionalinfo2p
--left join scores s on s.mainrecipecode=r.mainrecipecode and s.country=r.country
left join picklists_FR p on p.unique_recipe_code=r.uniquerecipecode
left join preference_FR as pf on pf.uniquerecipecode=r.uniquerecipecode
left join hqtag_FR as ht on ht.uniquerecipecode=r.uniquerecipecode
left join tag_FR as rt on rt.uniquerecipecode=r.uniquerecipecode
left join producttype_FR as pt on pt.uniquerecipecode=r.uniquerecipecode
left join weeks_FR as w on w.hellofresh_week=r.absolutelastused
--left join volumes v on v.code=r.mainrecipecode
--left join uploads.fr_product__2022q2budgets u on u.primaryprotein=r.primaryprotein
left join inactiveskus_FR as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
left join donotuseskus_FR as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
left join spicysku_FR as k on p.unique_recipe_code = k.unique_recipe_code
left join steps_INT as steps ON steps.unique_recipe_code = r.uniquerecipecode
where lower(r.status) in ('ready for menu planning','planned')
    and  case when lower(r.status)  in ('ready for menu planning') then r.uniquerecipecode not like '%NL%'
            else TRUE  end
    and  case when r.uniquerecipecode like '%NL%' then r.absolutelastused >='2021-W01'
            else TRUE  end
    and length (r.primaryprotein)>0
    and r.primaryprotein <>'N/A'
    and r.primaryprotein is not null
    and  r.country='FR'
) temp
where o=1)

,picklists_DACH as(
    select
    uniquerecipecode
    , group_concat(code," | ") as skucode
    , group_concat(display_name," | ") as skuname
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , sum(coalesce(singlepick,0)) as singlepick
    , count(distinct code) as skucount
    ,sum(quantity_to_order_2p) as pickcount
    from (
        select
        r.unique_recipe_code as uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , seasonality_score
        , singlepick
        , rs.quantity_to_order_2p
        from (select * from last_recipe_remps where remps_instance = 'DACH') as r
        join (select * from last_ingredient_group_remps where remps_instance = 'DACH') as ig
        on r.id = ig.ingredient_group__recipe
        join (select * from last_recipe_sku_remps where remps_instance = 'DACH') as rs
        on ig.id = rs.recipe_sku__ingredient_group
        join (select * from last_sku_remps where remps_instance = 'DACH') as sku
        on sku.id = rs.recipe_sku__sku
        left join (select * from seasonality_INT2 where country = 'DACH') as s on s.sku=sku.code
        left join uploads.gamp_dach_singlepicks as p on p.code= sku.code
        where rs.quantity_to_order_2p>0
        group by 1,2,3,4,5,6) t
    group by 1
)

, all_recipes_DACH as(
select * from(
select cast(r.id as varchar) as rempsid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode as code
       ,r.version
       ,r.status
       ,regexp_replace(r.title, '\t|\n', '') as title
       ,concat(r.title,coalesce(regexp_replace(r.subtitle, '\t|\n', ''),''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,'')) as subtitle
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
       --,case when n.saturated_fats is null then 0 else n.saturated_fats end as saturated_fats
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
       ,cast(right(difficultylevel,1) as int) as difficulty
       --,ht.name as hqtag
       --,rt.name as tag
       ,case when r.uniquerecipecode like 'M%' then 'Meister'
         when pf.name IS NULL or pf.name = '' then 'not available'
         else pf.name end as preference
       ,concat (ht.name,rt.name,pf.name)  as preftag
       ,pt.name as producttype
     ,p.skucode
     ,p.skuname
     ,p.skucount
     ,i.inactiveskus_count
     ,d.donotuseskus_count
     ,i.inactiveskus
     ,d.donotuseskus
     ,k.spicy_sku_count
     ,k.spicy_skus
     ,p.seasonalityrisk
     --,round(rc.cost_1p,2) as cost1p
     ,round(rc.cost_2p,2) as cost2p
     --,round(rc.cost_3p,2) as cost3p
     --,round(rc.cost_4p,2) as cost4p
     --,p.skucode as pricemissingskus -- (disregard as this is just a dummy)
     /*,p.pickcount
     ,p.singlepick
     ,coalesce(com.risk_index,1) as riskindex
     ,coalesce(com.nr_skus,5.5) as nrskus */
     ,r.lastused
     --,r.nextused
     ,r.absolutelastused
     --,case when w.hellofresh_running_week is NOT NULL then w.hellofresh_running_week else -1 end as absolutelastusedrunning
     ,case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end as isnewrecipe
     ,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
     ,r.isdefault as isdefault
     ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
     ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
     ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
     ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
     ,r.mainimageurl as image_url
from materialized_views.int_scm_analytics_remps_recipe as r
left join (select * from last_recipe_remps where remps_instance = 'DACH') as r2 on r2.unique_recipe_code=r.uniquerecipecode
left join (select * from last_cost_remps where remps_instance = 'DACH') as rc on rc.id=r2.recipe__recipe_cost
left join (select * from last_nutrition_remps where remps_instance = 'DACH') as n on n.id=r.nutritionalinfo2p
left join picklists_DACH p on p.uniquerecipecode=r.uniquerecipecode
left join (select * from preference_remps where remps_instance = 'DACH') as pf on pf.uniquerecipecode=r.uniquerecipecode
left join (select * from hqtag_remps where remps_instance = 'DACH') as ht on ht.uniquerecipecode=r.uniquerecipecode
left join (select * from tag_remps where remps_instance = 'DACH') as rt on rt.uniquerecipecode=r.uniquerecipecode
left join (select * from producttype_remps where remps_instance = 'DACH') as pt on pt.uniquerecipecode=r.uniquerecipecode
left join uploads.dach_goat_risk_complexity com on com.uniquerecipecode=r.uniquerecipecode
left join (select * from inactiveskus_remps where market = 'DACH') as i on p.uniquerecipecode = i.unique_recipe_code --and on p.skucode = i.skucode
left join (select * from donotuseskus_remps where market = 'DACH') as d on p.uniquerecipecode = d.unique_recipe_code --and on p.skucode = d.skucode
left join (select * from spicysku_remps where market = 'DACH') as k on p.uniquerecipecode = k.unique_recipe_code
left join steps_INT as steps ON steps.unique_recipe_code = r.uniquerecipecode
where lower(r.status)  in ('ready for menu planning','pool','rework')
    and  case when lower(r.status) in ('ready for menu planning','rework') then versionused is not NULL else TRUE end
    and rc.cost_2p >1.5
    and rc.cost_3p>0
    and rc.cost_4p>0
    and lower(r.title) not like '%not use%' and lower(r.title) not like '%wrong%'
    and length (primaryprotein)>0
    and primaryprotein <>'N/A'
    and  r.country='DACH'
    and right(r.uniquerecipecode,2)<>'CH'
    and r.uniquerecipecode not like 'TEST%'
    and r.uniquerecipecode not like 'HE%'
    and r.uniquerecipecode not like 'ADD%'
    and r.uniquerecipecode not like 'CO%'
    and r.uniquerecipecode not like 'XMAS%'
    and r.title not like 'PLACEH%'
    /*and  case when lower(r.status)  in ('ready for menu planning') then r.absolutelastused >='2019-W01'
                else TRUE  end
    and ncat.primary_protein is not NULL*/
) temp
where o=1)



select distinct * from all_recipes_DKSE
UNION ALL
select distinct * from all_recipes_NO
UNION ALL
select distinct * from all_recipes_IT
UNION
select distinct * from all_recipes_JP
UNION ALL
select distinct * from all_recipes_CA
UNION ALL
select distinct * from all_recipes_FR
UNION ALL
select distinct * from all_recipes_IE
UNION ALL
select distinct * from all_recipes_DACH
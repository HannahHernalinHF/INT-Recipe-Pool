----- DKSE Recipe Pool -----

WITH scores AS (
    SELECT 'DKSE'                                                       AS country
         , 'all'                                                        AS region
         , mainrecipecode
         , SUM(score * rating_count) / SUM(rating_count)                AS scorewoscm
         , SUM(score_wscm * rating_count_wscm) / SUM(rating_count_wscm) AS scorescm
    FROM (
             SELECT *
                  , dense_rank() over (partition BY mainrecipecode, region
                                ORDER BY hellofresh_week DESC) AS o
             FROM materialized_views.gamp_recipe_scores
             WHERE (region = 'DK' OR region = 'SE')
               AND score > 0
               AND rating_count > 50
         ) t
    WHERE o = 1
    GROUP BY 1, 2, 3
)
   , scores_dk AS (
    SELECT 'DKSE'                                                       AS country
         , 'dk'                                                         AS region
         , mainrecipecode
         , SUM(score * rating_count) / SUM(rating_count)                AS scorewoscm
         , SUM(score_wscm * rating_count_wscm) / SUM(rating_count_wscm) AS scorescm
    FROM (
             SELECT *
                  , dense_rank() over (partition BY mainrecipecode, region ORDER BY hellofresh_week DESC) AS o
             FROM materialized_views.gamp_recipe_scores
             WHERE rating_count > 50
               AND region = 'DK'
               AND country = 'DK') t
    WHERE o = 1
    GROUP BY 1, 2, 3
)
   , scores_se AS (
    SELECT 'DKSE'                                                       AS country
         , 'se'                                                         AS region
         , mainrecipecode
         , SUM(score * rating_count) / SUM(rating_count)                AS scorewoscm
         , SUM(score_wscm * rating_count_wscm) / SUM(rating_count_wscm) AS scorescm
    FROM (
             SELECT *
                  , dense_rank() over (partition BY mainrecipecode, region ORDER BY hellofresh_week DESC) AS o
             FROM materialized_views.gamp_recipe_scores
             WHERE score > 0
               AND rating_count > 50
               AND region = 'SE'
               AND country = 'SE') t
    WHERE o = 1
    GROUP BY 1, 2, 3
)
   , score_prefs_all AS (
    SELECT *
    FROM (
             SELECT *
                  , dense_rank() over (partition BY code, region ORDER BY hellofresh_week DESC) AS o
             FROM (
                      SELECT s.region                               AS country
                           , 'all'                                  AS region
                           , s.hellofresh_week
                           , split_part(s.uniquerecipecode, '-', 1) AS code
                           , CASE
                                 WHEN SUM(s.rating_count_classic) > 0 THEN
                                         SUM(s.score_classic * rating_count_classic) / SUM(s.rating_count_classic)
                                 ELSE 0 END                         AS scorewoscm_classic
                           , SUM(s.rating_count_classic)            AS rating_count_classic
                           , CASE
                                 WHEN SUM(s.rating_count_wscm_classic) > 0 THEN
                                         SUM(s.score_wscm_classic * s.rating_count_wscm_classic) /
                                         SUM(s.rating_count_wscm_classic)
                                 ELSE 0 END                         AS scorescm_classic
                           , CASE
                                 WHEN SUM(s.rating_count_family) > 0 THEN
                                         SUM(s.score_family * s.rating_count_family) / SUM(s.rating_count_family)
                                 ELSE 0 END                         AS scorewoscm_family
                           , SUM(s.rating_count_family)             AS rating_count_family
                           , CASE
                                 WHEN SUM(s.rating_count_wscm_family) > 0 THEN
                                         SUM(s.score_wscm_family * s.rating_count_wscm_family) /
                                         SUM(s.rating_count_wscm_family)
                                 ELSE 0 END                         AS scorescm_family
                           , CASE
                                 WHEN SUM(s.rating_count_veggie) > 0 THEN
                                         SUM(s.score_veggie * s.rating_count_veggie) / SUM(s.rating_count_veggie)
                                 ELSE 0 END                         AS scorewoscm_veggie
                           , SUM(s.rating_count_veggie)             AS rating_count_veggie
                           , CASE
                                 WHEN SUM(s.rating_count_wscm_veggie) > 0 THEN
                                         SUM(s.score_wscm_veggie * s.rating_count_wscm_veggie) /
                                         SUM(s.rating_count_wscm_veggie)
                                 ELSE 0 END                         AS scorescm_veggie
                           , CASE
                                 WHEN SUM(s.rating_count_quick) > 0 THEN
                                         SUM(s.score_quick * s.rating_count_quick) / SUM(s.rating_count_quick)
                                 ELSE 0 END                         AS scorewoscm_quick
                           , SUM(s.rating_count_quick)              AS rating_count_quick
                           , CASE
                                 WHEN SUM(s.rating_count_wscm_quick) > 0 THEN
                                         SUM(s.score_wscm_quick * s.rating_count_wscm_quick) /
                                         SUM(s.rating_count_wscm_quick)
                                 ELSE 0 END                         AS scorescm_quick
                      FROM views_analysts.gamp_dkse_pref_scores s
                      WHERE s.region = 'DKSE'
                      GROUP BY 1, 2, 3, 4
                  ) t
             WHERE t.rating_count_family > 50
                OR t.rating_count_veggie > 50
                OR rating_count_classic > 50
         ) x
    WHERE o = 1
)
   , volumes AS (
    SELECT code
         , round(AVG(last_region_share), 4)   AS volume_share_last
         , round(AVG(last_2_region_share), 4) AS volume_share_2_last
         , SUM(last_count)                    AS last_count
         , SUM(last_2_count)                  AS last_2_count
    FROM views_analysts.gamp_recipe_volumes
    WHERE (country = 'DK' OR country = 'SE')
    GROUP BY 1
)
   , seasonality AS (
    SELECT sku
         , MAX(seasonality_score) AS seasonality_score
    FROM uploads.gp_sku_seasonality
    WHERE country IN ('NORDICS')
      --AND week >= 'W39'
      --AND week <= 'W65'
    GROUP BY 1
)
   , recipe_usage AS (
    SELECT *
         , CASE
               WHEN last_used_running_week IS NOT NULL AND next_used_running_week IS NOT NULL
                   THEN next_used_running_week - last_used_running_week
               ELSE 0 END AS lastnextuseddiff
    FROM materialized_views.isa_services_recipe_usage r
    WHERE r.market = 'dkse'
      AND r.region_code = 'se'
)
   , nutrition AS (
    SELECT *
    FROM materialized_views.culinary_services_recipe_segment_nutrition
    WHERE market = 'dkse'
      AND segment IN ('SE')
)
   , cost AS (
    SELECT recipe_id
    , size
   , AVG (price) AS cost
    FROM materialized_views.culinary_services_recipe_static_price
    WHERE segment = 'SE'
      AND distribution_center = 'SK'
      --AND hellofresh_week >= '2022-W37'
      --AND hellofresh_week <= '2022-W65'
    GROUP BY 1, 2
    )
    , sku_cost AS (
        SELECT code
                , AVG (price) AS price
        FROM materialized_views.procurement_services_staticprices sp
            LEFT JOIN materialized_views.procurement_services_culinarysku sku
        ON sku.id=sp.culinary_sku_id
        WHERE sku.market='dkse'
          AND sp.distribution_center='SK'
          --AND sp.hellofresh_week >= '2022-W37'
          --AND sp.hellofresh_week <= '2022-W65'
        GROUP BY 1
    )
    , picklists AS (
        SELECT unique_recipe_code
                , group_concat(code, " | ") AS skucode
                , group_concat(NAME, " | ") AS skuname
                , MAX (COALESCE (seasonality_score, 0)) AS seasonalityrisk
                , SUM (COALESCE (boxitem, 0)) AS boxitem
                , COUNT (DISTINCT code) AS skucount
                , SUM (cost1p) AS cost1p
                , SUM (cost2p) AS cost2p
                , SUM (cost3p) AS cost3p
                , SUM (cost4p) AS cost4p
                , group_concat(price_missing, " | ") AS pricemissingskus
                , group_concat(status, " | ") as sku_status
        FROM (
            SELECT r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , seasonality_score
                , boxitem
                , CASE WHEN price IS NULL OR price=0 THEN p.code ELSE NULL END AS price_missing
                , SUM (CASE WHEN SIZE = 1 THEN pick_count * price ELSE 0 END) AS cost1p
                , SUM (CASE WHEN SIZE = 2 THEN pick_count * price ELSE 0 END) AS cost2p
                , SUM (CASE WHEN SIZE = 3 THEN pick_count * price ELSE 0 END) AS cost3p
                , SUM (CASE WHEN SIZE = 4 THEN pick_count * price ELSE 0 END) AS cost4p
                , skus.status
            FROM materialized_views.isa_services_recipe_consolidated r
            JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
            ON r.id = p.recipe_id
            LEFT JOIN materialized_views.procurement_services_culinarysku as skus
               ON skus.id = p.culinarysku_id
            LEFT JOIN sku_cost C
            ON C.code = p.code
            LEFT JOIN seasonality s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name ='SE'
            GROUP BY 1, 2, 3, 4, 5, 6, 11 ) t
        GROUP BY 1
    )

 , skucount_2p as (
        SELECT unique_recipe_code
                , group_concat(code, " | ") AS skucode
                , group_concat(NAME, " | ") AS skuname
                , MAX (COALESCE (seasonality_score, 0)) AS seasonalityrisk
                , SUM (COALESCE (boxitem, 0)) AS boxitem
                , COUNT (DISTINCT code) AS skucount
                , SUM (cost1p) AS cost1p
                , SUM (cost2p) AS cost2p
                , SUM (cost3p) AS cost3p
                , SUM (cost4p) AS cost4p
                , group_concat(price_missing, " | ") AS pricemissingskus
                , group_concat(status, " | ") as sku_status
                , size
        FROM (
            SELECT r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') AS NAME
                , seasonality_score
                , boxitem
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
            LEFT JOIN sku_cost C
            ON C.code = p.code
            LEFT JOIN seasonality s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name ='SE'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12 ) t
        GROUP BY 1,13
    )

, inactiveskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(skuname," | ") AS inactiveskus,
        count(skuname) AS inactiveskus_count
    FROM (
            SELECT r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , seasonality_score
                , boxitem
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
            LEFT JOIN sku_cost C
            ON C.code = p.code
            LEFT JOIN seasonality s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name ='SE'
            AND skus.status LIKE '%Inactive%' OR skus.status LIKE '%Archived%'
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12
         ) t
    GROUP BY 1,2--, skus.code
    )

, donotuseskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(skuname," | ") AS donotuseskus,
        count(skuname) AS donotuseskus_count
    FROM (
            SELECT r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , seasonality_score
                , boxitem
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
            LEFT JOIN sku_cost C
            ON C.code = p.code
            LEFT JOIN seasonality s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name ='SE'
            AND p.name LIKE '%DO NOT USE%' OR p.name LIKE '%do not use%'
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12
         ) t
    GROUP BY 1,2--, skus.code
    )

, spicysku as (
    SELECT unique_recipe_code,
           count(distinct skuname) AS spicy_sku_count,
           group_concat(distinct skuname, " | ") AS spicy_skus
    FROM (
            SELECT r.unique_recipe_code
                , p.code as skucode
                , regexp_replace(p.name, '\t|\n', '') AS skuname
                , seasonality_score
                , boxitem
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
            LEFT JOIN sku_cost C
            ON C.code = p.code
            LEFT JOIN seasonality s
            ON s.sku = p.code
            LEFT JOIN uploads.gamp_dkse_boxitems b
            ON b.code= p.code
            WHERE r.market = 'dkse'
            AND p.segment_name ='SE'
            AND  p.name LIKE '%chili / chili /chili/ Chili%'
                 OR p.name LIKE '%chili%'
                 OR p.name LIKE '%Chili%'
                 OR p.name LIKE '%chilli%'
                 OR p.name LIKE '%Sriracha sauce%'
                 OR p.name LIKE '%sriracha%'
                 OR p.name LIKE '%Jalapeno, Green, Medium Spicy%'
                 OR p.name LIKE '%jalapeno%'
                 OR p.name LIKE '%Sriracha Mayo%'
                 OR p.name LIKE '%Chorizo Sausage%'
                 OR p.name LIKE '%Chili, Dried%'
                 OR p.name LIKE '%wasabi%'
                 OR p.name LIKE '%karashi%'
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12
         ) t
    GROUP BY 1    )

, filtered_recipes as (
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
                , p.boxitem
                , dense_rank() over (partition by r.recipe_code, r.market
                                        order by r.version  desc) as o
        from materialized_views.isa_services_recipe_consolidated as r
            left join picklists as p
                on p.unique_recipe_code=r.unique_recipe_code
        where   r.market = 'dkse'
                and length (r.primary_protein)>0
                and r.primary_protein <>'N/A'
                and p.cost2p > 0
    ) temp
    where temp.o = 1
)
, score_prediction as (
    select r.*
        , case  when scorescm is not NULL
                    then scorescm
                when avg(scorescm) over (partition by r.primary_protein, r.country ) is not NULL
                    THEN avg(scorescm) over (partition by r.primary_protein, r.country)
                when avg(scorescm) over (partition by split_part(r.primary_protein,'-',1), r.country ) is not NULL
                    THEN avg(scorescm) over (partition by split_part(r.primary_protein,'-',1), r.country )
                else 3.4
            end as scorescm
       , case when scorewoscm is not NULL then scorewoscm
             when avg(scorewoscm) over (partition by r.primary_protein, r.country ) is not NULL
                 THEN avg(scorewoscm) over (partition by r.primary_protein, r.country)
             when avg(scorewoscm) over (partition by split_part(r.primary_protein,'-',1), r.country ) is not NULL
                 THEN avg(scorewoscm) over (partition by split_part(r.primary_protein,'-',1), r.country )
             else 3.4
            end as scorewoscm
        , case when s.scorewoscm is  NULL
                    then 1
                else 0
            end as isscorereplace
    from filtered_recipes r
        left join scores s
            on s.mainrecipecode = r.recipe_code
                and s.country = r.country
)

, steps as (
    SELECT r.id,
           steps.recipe_id,
           group_concat(steps.title, " | ") AS step_title,
           group_concat(steps.description," | ") as step_description
    FROM materialized_views.culinary_services_recipe_steps_translations as steps
    JOIN materialized_views.isa_services_recipe_consolidated as r
        ON r.id = steps.recipe_id
    GROUP BY 1,2
    )

, all_recipes as(
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
            --, r.skucount
            , sc2p.skucount
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
            --, s.scorescm
            --, s.scorewoscm
            --, coalesce(s_dk.scorewoscm,0) as scorewoscmdk
            --, coalesce(s_se.scorewoscm,0) as scorewoscmse
            --, coalesce(s_dk.scorescm,0) as scorescmdk
            --, coalesce(s_se.scorescm,0) as scorescmse
            --, s.isscorereplace
            --, coalesce(spa.scorewoscm_classic,0) as scorewoscm_classic
            --, coalesce(spa.scorescm_classic,0) as scorescm_classic
            --, coalesce(spa.scorewoscm_family,0) as scorewoscm_family
            --, coalesce(spa.scorescm_family,0) as scorescm_family
            --, coalesce(spa.scorewoscm_quick,0) as scorewoscm_quick
            --, coalesce(spa.scorescm_quick,0) as scorescm_quick
            --, coalesce(spa.scorewoscm_veggie,0)  as scorewoscm_veggie
            --, coalesce(spa.scorescm_veggie,0)  as scorescm_veggie
            --, coalesce(v.volume_share_last,0) as volumesharelast
            --, coalesce(v.volume_share_2_last,0) as volumeshare2last
            , r.o
            , r.updated_at as updated_at
            , case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            , case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
    from filtered_recipes as r
        left join recipe_usage u
            on u.recipe_code = r.recipe_code
        left join nutrition n
            on n.recipe_id = r.id
        left join (select * from cost where size=1) rc_1
            on rc_1.recipe_id=r.id
        left join (select * from cost where size=2) rc_2
            on rc_2.recipe_id=r.id
        left join (select * from cost where size=3) rc_3
            on rc_3.recipe_id=r.id
        left join (select * from cost where size=4) rc_4
            on rc_4.recipe_id=r.id
        left join score_prediction as s
            on s.recipe_code=r.recipe_code
        left join scores_dk as s_dk
            on s_dk.mainrecipecode=r.recipe_code
            and s_dk.country=r.country
        left join scores_se as s_se
            on s_se.mainrecipecode=r.recipe_code
            and s_se.country=r.country
        left join score_prefs_all as spa
            on spa.code=r.recipe_code
            and spa.country=r.country
        left join volumes as v
            on v.code=r.recipe_code
        left join picklists p
                on p.unique_recipe_code=r.unique_recipe_code
        left join skucount_2p as sc2p
                on sc2p.unique_recipe_code=r.unique_recipe_code
        left join inactiveskus as i
                on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
        left join donotuseskus as d
                on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
        left join spicysku as k
                on p.unique_recipe_code = k.unique_recipe_code
        left join steps ON steps.recipe_id = r.id
    where lower(r.status) not in ('inactive','rejected')
)

select * from all_recipes



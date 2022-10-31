----- NO Recipe Pool -----

    with scores as(
    select region as country
         , region as region
         , mainrecipecode
         , sum(rating_count) as rating_count
         , sum(score*rating_count)/sum(rating_count) as scorewoscm
         , sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    from(
        select *
            ,dense_rank() over (partition by mainrecipecode, region
                                order by hellofresh_week desc) as o
        from materialized_views.gamp_recipe_scores
        where region='NO'
            and score>0 and rating_count>10
        ) t
        where o=1
    group by 1,2,3
)

, score_prefs_all as(
    select *
    from (
        select *
            , dense_rank() over (partition by code, region order by hellofresh_week desc) as o
        from
        (
            select s.region as country
                , 'all' as region
                , s.hellofresh_week
                , split_part(s.uniquerecipecode,'-',1) as code
                , sum(s.score_classic*s.rating_count_classic)/sum(s.rating_count_classic) as scorewoscm_classic
                , sum(s.rating_count_classic) as rating_count_classic
                , sum(s.score_wscm_classic*s.rating_count_wscm_classic)/sum(s.rating_count_wscm_classic) as scorescm_classic
                , sum(s.score_family*s.rating_count_family)/sum(s.rating_count_family) as scorewoscm_family
                , sum(s.rating_count_family) as rating_count_family
                , sum(s.score_wscm_family*s.rating_count_wscm_family)/sum(s.rating_count_wscm_family) as scorescm_family
                , sum(s.score_veggie*s.rating_count_veggie)/sum(s.rating_count_veggie) as scorewoscm_veggie
                , sum(s.rating_count_veggie) as rating_count_veggie
                , sum(s.score_wscm_veggie*s.rating_count_wscm_veggie)/sum(s.rating_count_wscm_veggie) as scorescm_veggie
                , sum(s.score_quick*s.rating_count_quick)/sum(s.rating_count_quick) as scorewoscm_quick
                , sum(s.rating_count_quick) as rating_count_quick
                , sum(s.score_wscm_quick*s.rating_count_wscm_quick)/sum(s.rating_count_wscm_quick) as scorescm_quick
            from views_analysts.gamp_no_pref_scores s
            where s.region='NO'
            group by 1,2,3,4
        )t
        where t.rating_count_family > 10
            or t.rating_count_veggie > 10
            or t.rating_count_quick > 10
            or t.rating_count_classic > 10
    )x
    where o=1
)
, volumes as(
    select  code
         , round(avg(last_region_share),4) as volume_share_last
         , round(avg(last_2_region_share),4) as volume_share_2_last
         , sum(last_count) as last_count
         , sum(last_2_count) as last_2_count
    from views_analysts.gamp_recipe_volumes
        where country='NO'
    group by 1
)
, seasonality as(
    select sku
         , max(seasonality_score) as seasonality_score
    from uploads.gp_sku_seasonality
        where  country in ('NORDICS')
          --and week>='W35' and week<='W60'
    group by 1
)
, recipe_usage as(
    select *
            ,case when last_used_running_week is not NULL and next_used_running_week is not NULL
                  then next_used_running_week - last_used_running_week
                   else 0 end as lastnextuseddiff
    from materialized_views.isa_services_recipe_usage r
    where r.market = 'dkse'
        and r.region_code = 'no'
)
, nutrition as(
    select *
    from materialized_views.culinary_services_recipe_segment_nutrition
    where market = 'dkse'
        and segment in ('NO')
)
,cost as(
    select recipe_id, size
        , avg(price) as cost
    from materialized_views.culinary_services_recipe_static_price
    where segment = 'NO' and distribution_center = 'MO'
        --and hellofresh_week >= '2022-W35' and hellofresh_week <= '2022-W60'
    group by 1,2
)
, sku_cost as(
    select code
      ,  avg(price) as price
    from materialized_views.procurement_services_staticprices sp
        left join materialized_views.procurement_services_culinarysku sku
            on sku.id=sp.culinary_sku_id
    where  sku.market='dkse'
        and sp.distribution_center='MO'
        --and sp.hellofresh_week >=  '2022-W35' and sp.hellofresh_week <= '2022-W60'
    group by 1
)
, picklists as(
    select unique_recipe_code
            , group_concat(code," | ") as skucode
            , group_concat(name," | ") as skuname
            , max(coalesce(seasonality_score,0)) as seasonalityrisk
            , sum(coalesce(boxitem,0)) as boxitem
            , count(distinct code) as skucount
            , sum(cost1p) as cost1p
            , sum(cost2p) as cost2p
            , sum(cost3p) as cost3p
            , sum(cost4p) as cost4p
            , group_concat(price_missing," | ") as pricemissingskus
        from (
            select  r.unique_recipe_code
                , p.code
                , regexp_replace(p.name, '\t|\n', '') as name
                , seasonality_score
                , boxitem
                , case when price is null or price=0 then p.code else NULL end as price_missing
                , sum(case when size = 1 then pick_count * price else 0 end) as cost1p
                , sum(case when size = 2 then pick_count * price else 0 end) as cost2p
                , sum(case when size = 3 then pick_count * price else 0 end) as cost3p
                , sum(case when size = 4 then pick_count * price else 0 end) as cost4p
            from materialized_views.isa_services_recipe_consolidated r
                join materialized_views.culinary_services_recipe_procurement_picklist_culinarysku p
                    on r.id = p.recipe_id
                left join sku_cost c
                    on c.code = p.code
                left join seasonality s
                    on s.sku = p.code
                left join uploads.gamp_dkse_boxitems b
                    on b.code= p.code
            where r.market = 'dkse'
                and p.segment_name ='NO'
            group by 1,2,3,4,5,6 ) t
        group by 1
)


 , skucount_2p as (
        SELECT unique_recipe_code
                , group_concat(code, " | ") AS skucode
                , group_concat(distinct NAME, " | ") AS skuname
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
            AND p.segment_name = 'NO'
            AND p.size = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12 ) t
        GROUP BY 1,13
    )

, inactiveskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(distinct skuname," | ") AS inactiveskus,
        count(distinct skuname) AS inactiveskus_count
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
            AND p.segment_name = 'NO'
            AND skus.status LIKE '%Inactive%' OR skus.status LIKE '%Archived%'
            GROUP BY 1, 2, 3, 4, 5, 6, 11, 12
         ) t
    GROUP BY 1,2--, skus.code
    )

, donotuseskus as (
    SELECT unique_recipe_code,
        skucode,
        group_concat(distinct skuname," | ") AS donotuseskus,
        count(distinct skuname) AS donotuseskus_count
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
            WHERE r.market  = 'dkse'
            AND p.segment_name = 'NO'
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
            WHERE r.market  = 'dkse'
            AND p.segment_name = 'NO'
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
        , rating_count
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

, allergens as (
SELECT c.country,
       a.culinary_sku_id,
       b.id,
       c.ingredient_id,
       c.recipe_id,
       group_concat(distinct a.allergens,", ") as allergens
FROM materialized_views.procurement_services_suppliersku_nutrition as a
JOIN materialized_views.procurement_services_culinarysku as b
    ON b.id = a.culinary_sku_id
JOIN materialized_views.culinary_services_recipe_ingredients_in_recipe  as c
    ON c.ingredient_id = b.ingredient_id
GROUP BY 1,2,3,4,5
)

, all_recipes as(
    select r.id as uuid
            , r.country
            , r.unique_recipe_code as uniquerecipecode
            , r.recipe_code as code
            , r.version
            , r.status
            , regexp_replace(r.title, '\t|\n', '') as title
            , concat(regexp_replace(r.title, '\t|\n', ''), coalesce(regexp_replace(r.subtitle, '\t|\n', ''),'') ,coalesce (r.primary_protein,''),coalesce(r.primary_starch,''),coalesce(r.cuisine,''), coalesce(r.dish_type,''), coalesce(r.primary_vegetable,'')) as subtitle
            , CASE WHEN r.primary_protein IS NULL OR r.primary_protein = '' THEN 'not available' ELSE r.primary_protein END AS primaryprotein
            , r.main_protein as mainprotein
            , r.protein_cut as proteincut
            , coalesce(r.secondary_protein,'none') as secondaryprotein
            , r.proteins
            , CASE WHEN r.primary_starch IS NULL OR r.primary_starch = '' THEN 'not available' ELSE r.primary_starch END AS primarystarch
            , r.main_starch as mainstarch
            , coalesce(r.secondary_starch,'none') as secondarystarch
            , r.starches
            , CASE WHEN coalesce(r.primary_vegetable,'none') IS NULL OR coalesce(r.primary_vegetable,'none') = '' THEN 'not available' ELSE coalesce(r.primary_vegetable,'none') END as primaryvegetable
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
            , CASE WHEN r.cuisine IS NULL OR r.cuisine = '' THEN 'not available' ELSE r.cuisine END AS cuisine
            , CASE WHEN r.dish_type IS NULL OR r.dish_type = '' THEN 'not available' ELSE r.dish_type END as dishtype
            , case when r.hands_on_time ="" or r.hands_on_time is NULL then cast(99 as float)
                else cast(r.hands_on_time as float) end as handsontime
            , case when r.hands_on_time ="" or r.hands_on_time is NULL then cast(99 as float)
                 else cast(r.hands_on_time as float) end
                  +
              case when r.hands_off_time ="" or r.hands_off_time is NULL then cast(99 as float)
                 else cast(r.hands_off_time as float) end
                  as totaltime
            , r.difficulty
            --, r.tags as tag
            , CASE WHEN r.target_preferences IS NULL OR r.target_preferences = '' THEN 'not available' ELSE r.target_preferences END AS preference
            , concat (r.tags,r.target_preferences) as preftag
            , r.recipe_type as recipetype
            , r.skucode
            , lower(r.skuname) as skuname
            , p.skucount
            , i.inactiveskus_count
            , d.donotuseskus_count
            , i.inactiveskus
            , d.donotuseskus
            , k.spicy_sku_count
            , k.spicy_skus
            , r.seasonalityrisk
            --, r.target_products as producttype
            --, r.recipe_type as recipetype
            --, r.created_by as author
            --, r.label
            --, r.cost1p
            , r.cost2p
            --, r.cost3p
            , r.cost4p
            , r.pricemissingskus
            --, r.boxitem
            --, s.scorescm
            --, s.scorewoscm
            --, coalesce (s.rating_count,0) as ratingcount
            --, isscorereplace
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
            , dense_rank() over (partition by r.recipe_code, r.market order by r.version  desc) as o
            , r.updated_at as updated_at
            ,case when steps.step_title IS NULL or steps.step_title LIKE '% |  |  %' then 'not available' else steps.step_title end as step_title
            ,case when steps.step_description IS NULL or steps.step_description LIKE '% |  |  %' then 'not available' else steps.step_description end as step_description
           -- , al.allergens
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
        left join score_prefs_all as spa
            on spa.code=r.recipe_code
            and spa.country=r.country
        left join volumes as v
            on v.code=r.recipe_code
        left join picklists p on p.unique_recipe_code=r.unique_recipe_code
        left join skucount_2p as sc2p on sc2p.unique_recipe_code=r.unique_recipe_code
        left join inactiveskus as i on p.unique_recipe_code = i.unique_recipe_code --and on p.skucode = i.skucode
        left join donotuseskus as d on p.unique_recipe_code = d.unique_recipe_code --and on p.skucode = d.skucode
        left join spicysku as k on p.unique_recipe_code = k.unique_recipe_code
        left join steps ON steps.recipe_id = r.id
        left join allergens as al on r.id = al.recipe_id
    where lower(r.status) not in ('inactive','rejected')
)

select distinct * from all_recipes
----- INT REMPS RECIPE POOL -----

----- DACH GAMP Recipe Pool Query -----

with scores as (select region                                                       as country
                     , mainrecipecode
                     , sum(score * rating_count) / sum(rating_count)                as scorewoscm
                     , sum(score_wscm * rating_count_wscm) / sum(rating_count_wscm) as scorescm
                     , sum(count_of_1s) / sum(rating_count)                         as share1
                     , sum(count_of_1s_wscm) / sum(rating_count_wscm)               as share1scm
                from (select *
                           , dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
                      from materialized_views.gamp_recipe_scores
                      where region = 'DACH'
                        and country = 'DE'
                        and score > 0
                        and rating_count > 100) t
                where o = 1
                group by 1, 2)
-------
------ UPDATE THE PLANNING INTERVAL !!!!!!!
-------
   , seasonality as (select sku, max(seasonality_score) as seasonality_score
                     from uploads.gp_sku_seasonality
                     where country = 'DACH' --and week>='{seasonfirst}'and week<='{seasonlast}'
                     group by 1)

   , volumes as (select code,
                        round(avg(last_region_share), 4)   as volume_share_last,
                        round(avg(last_2_region_share), 4) as volume_share_2_last,
                        sum(last_count)                    as last_count,
                        sum(last_2_count)                  as last_2_count
                 from views_analysts.gamp_recipe_volumes
                 where region = 'DACH'
                   and country = 'DE'
                 group by 1)

   , last_recipe as (select *
                     from (select *,
                                  dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                           from remps.recipe_recipes
                           where remps_instance = 'DACH') t
                     where o = 1)

   , last_cost as (select *
                   from (select *,
                                dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                         from remps.recipe_recipecost
                         where remps_instance = 'DACH') t
                   where o = 1)

   , last_nutrition AS (SELECT *
                        FROM (SELECT *,
                                     dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                              FROM remps.recipe_nutritionalinfopp
                              WHERE remps_instance = 'DACH') AS t
                        WHERE o = 1)

   , last_nutrition_cat AS (SELECT *
                            FROM (SELECT *,
                                         dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                                  FROM remps.recipe_nutritionalinfo
                                  WHERE remps_instance = 'DACH') AS t
                            WHERE o = 1)

   , last_category AS (SELECT *
                       FROM (SELECT *,
                                    dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                             FROM remps.recipe_recipecategory
                             WHERE remps_instance = 'DACH') AS t
                       WHERE o = 1)


   , last_tag as (select *
                  from (select *,
                               dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                        from remps.recipetags_tags
                        where remps_instance = 'DACH') t
                  where o = 1)

   , last_tag_map as (select *
                      from (select *,
                                   dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                            from remps.map_tags_recipes
                            where remps_instance = 'DACH') t
                      where o = 1)


   , last_product as (select *
                      from (select *,
                                   dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                            from remps.recipe_producttypes
                            where remps_instance = 'DACH') t
                      where o = 1)

   , last_preference as (select *
                         from (select *,
                                      dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                               from remps.recipetags_recipepreferences
                               where remps_instance = 'DACH') t
                         where o = 1)

   , last_preference_map as (select *
                             from (select *,
                                          dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                                   from remps.map_recipepreferences_recipes
                                   where remps_instance = 'DACH') t
                             where o = 1)

   , last_hqtag as (select *
                    from (select *,
                                 dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                          from remps.recipetags_hqtags
                          where remps_instance = 'DACH') t
                    where o = 1)

   , last_hqtag_map as (select *
                        from (select *,
                                     dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                              from remps.map_hqtags_recipes
                              where remps_instance = 'DACH') t
                        where o = 1)
   , last_ingredient_group as (SELECT *
                               FROM (SELECT *,
                                            dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                                     from remps.recipe_ingredientgroup
                                     WHERE remps_instance = 'DACH') AS t
                               WHERE o = 1)

   , last_recipe_sku as (SELECT *
                         FROM (SELECT *,
                                      dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                               from remps.recipe_recipeskus
                               WHERE remps_instance = 'DACH') AS t
                         WHERE o = 1)

   , last_sku as (SELECT *
                  FROM (SELECT *,
                               dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
                        from remps.sku_sku
                        WHERE remps_instance = 'DACH') AS t
                  WHERE o = 1)


   , picklists as (select uniquerecipecode
                        , group_concat(code, " | ")           as skucode
                        , group_concat(display_name, " | ")   as skuname
                        , max(coalesce(seasonality_score, 0)) as seasonalityrisk
                        , sum(coalesce(singlepick, 0))        as singlepick
                        , count(distinct code)                as skucount
                        , sum(quantity_to_order_2p)           as pickcount
                   from (select r.unique_recipe_code                          as uniquerecipecode
                              , sku.code
                              , regexp_replace(sku.display_name, '\t|\n', '') as display_name
                              , seasonality_score
                              , singlepick
                              , rs.quantity_to_order_2p
                         from last_recipe r
                                  join last_ingredient_group ig
                                       on r.id = ig.ingredient_group__recipe
                                  join last_recipe_sku rs
                                       on ig.id = rs.recipe_sku__ingredient_group
                                  join last_sku sku
                                       on sku.id = rs.recipe_sku__sku
                                  left join seasonality s on s.sku = sku.code
                                  left join uploads.gamp_dach_singlepicks p on p.code = sku.code
                         where rs.quantity_to_order_2p > 0
                         group by 1, 2, 3, 4, 5, 6) t
                   group by 1)

   , hqtag as (select rr.unique_recipe_code                                     as uniquerecipecode,
                      coalesce(group_concat(distinct rt.original_name,','), '') as name
               from last_recipe rr
                        left join last_hqtag_map m on rr.id = m.recipe_recipes_id
                        left join last_hqtag rt on rt.id = m.recipetags_hqtags_id
               group by 1)

   , preference as (select rr.unique_recipe_code                            as uniquerecipecode,
                           coalesce(group_concat(distinct rp.name,','), '') as name
                    from last_recipe rr
                             left join last_preference_map m on rr.id = m.recipe_recipes_id
                             left join last_preference rp on rp.id = m.recipetags_recipepreferences_id
                    group by 1)

   , producttype as (select rr.unique_recipe_code                            as uniquerecipecode,
                            coalesce(group_concat(distinct rp.name,','), '') as name
                     from last_recipe rr
                              left join last_product rp on rp.id = rr.recipe__product_type
                     group by 1)
   , tag as (select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','), '') as name
             from last_recipe rr
                      left join last_tag_map m on rr.id = m.recipe_recipes_id
                      left join last_tag rt on rt.id = m.recipetags_tags_id
             group by 1)

   , weeks as (select distinct hellofresh_week, hellofresh_running_week
               from dimensions.date_dimension)

   , all_recipes_DACH as (select *
                     from (select r.id                                                                         as rempsid
                                , r.country
                                , r.uniquerecipecode
                                , r.mainrecipecode                                                             as code
                                , r.version
                                , r.status
                                , regexp_replace(r.title, '\t|\n', '')                                         as title
                                , concat(r.title, coalesce(regexp_replace(r.subtitle, '\t|\n', ''), ''),
                                         coalesce(r.primaryprotein, ''), coalesce(r.primarystarch, ''),
                                         coalesce(r.cuisine, ''),
                                         coalesce(r.dishtype, ''))                                             as subtitle
                                --, r.lastused
                                --, r.nextused
                                , r.absolutelastused
                                /*, case
                                      when w.hellofresh_running_week is NOT NULL then w.hellofresh_running_week
                                      else -1 end                                                              as absolutelastusedrunning*/
                                , case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end        as isnewrecipe
                                , case when r.nextused is not NULL and r.lastused is NULL then 1 else 0 end    as isnewscheduled
                                , r.isdefault                                                                  as isdefault
                                , r.primaryprotein
                                --, TRIM(coalesce(split_part(r.primaryprotein, '-', 1), r.primaryprotein))       as mainprotein
                                --, TRIM(coalesce(split_part(r.primaryprotein, '-', 2), r.primaryprotein))       as proteincut
                                --, coalesce(r.secondaryprotein, 'none')                                         as secondaryprotein
                                --, concat(coalesce(r.primaryprotein, ''),
                                --         coalesce(r.secondaryprotein, 'none'))                                 as proteins
                                , r.primarystarch
                                --, coalesce(TRIM(coalesce(split_part(r.primarystarch, '-', 1), r.primarystarch)),
                                --           'none')                                                             as mainstarch
                                --, coalesce(r.secondarystarch, 'none')                                          as secondarystarch
                                --, concat(coalesce(r.primarystarch, ''),
                                --         coalesce(r.secondarystarch, 'none'))                                  as starches
                                , coalesce(r.primaryvegetable, 'none')                                         as primaryvegetable
                                --, coalesce(TRIM(coalesce(split_part(r.primaryvegetable, '-', 1), r.primaryvegetable)),
                                --           'none')                                                             as mainvegetable
                                --, concat(coalesce(r.primaryvegetable, 'none'), coalesce(r.secondaryvegetable, 'none'),
                                --         coalesce(r.tertiaryvegetable, 'none'))                                as vegetables
                                --, coalesce(r.secondaryvegetable, 'none')                                       as secondaryvegetable
                                --, coalesce(r.tertiaryvegetable, 'none')                                        as tertiaryvegetable
                                --, coalesce(r.primarydryspice, 'none')                                          as primarydryspice
                                --, coalesce(r.primarycheese, 'none')                                            as primarycheese
                                --, coalesce(r.primaryfruit, 'none')                                             as primaryfruit
                                --, coalesce(r.primarydairy, 'none')                                             as primarydairy
                                --, coalesce(r.primaryfreshherb, 'none')                                         as primaryfreshherb
                                --, coalesce(r.primarysauce, 'none')                                             as primarysauce
                                --, case when n.salt is null then 0 else n.salt end                              as salt
                                , case when n.kilo_calories = 0 then 999 else n.kilo_calories end              as calories
                                --, case when n.saturated_fats is null then 0 else n.saturated_fats end          as saturated_fats
                                , r.cuisine
                                , r.dishtype
                                /*, case
                                      when r.handsontime = "" or r.handsontime is NULL then cast(99 as float)
                                      when length(r.handsontime) > 3 and cast(left (r.handsontime, 2) as float) is NULL
                                          then 99
                                      when length(r.handsontime) > 3 and
                                           cast(left (r.handsontime, 2) as float) is not NULL
                                          then cast(left (r.handsontime, 2) as float)
                                      when length(r.handsontime) < 2 then cast(99 as float)
                                      when r.handsontime = '0' then cast(99 as float)
                                      else cast(r.handsontime as float) end                                    as handsontime */
                                , case
                                      when r.totaltime = "" or r.totaltime is NULL then cast(99 as float)
                                      when length(r.totaltime) > 3 and cast(left (r.totaltime, 2) as float) is NULL
                                          then 99
                                      when length(r.totaltime) > 3 and cast(left (r.totaltime, 2) as float) is not NULL
                                          then cast(left (r.totaltime, 2) as float)
                                      when length(r.totaltime) < 2 then cast(99 as float)
                                      when r.totaltime = '0' then cast(99 as float)
                                      else cast(r.totaltime as float) end                                      as totaltime
                                , difficultylevel                                                              as difficulty
                                --, ht.name                                                                      as hqtag
                                , rt.name                                                                      as tag
                                , case when r.uniquerecipecode like 'M%' then 'Meister' else pf.name end       as preference
                                , concat(ht.name, rt.name, pf.name)                                            as preftag
                                , pt.name                                                                      as producttype
                                --, cat.name                                                                     as recipecategory
                                --, r.author
                                , round(rc.cost_1p, 2)                                                         as cost1p
                                , round(rc.cost_2p, 2)                                                         as cost2p
                                , round(rc.cost_3p, 2)                                                         as cost3p
                                , round(rc.cost_4p, 2)                                                         as cost4p
                                --, r.lastscore
                                , case when s.scorescm is not NULL then s.scorescm
                                      when avg(s.scorescm) over (partition by r.primaryprotein, r.country ) is not NULL
                                      then avg(s.scorescm) over (partition by r.primaryprotein, r.country )
                                      when avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                                      then avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
                                  else 3.4
                                  end as scorescm
                                , case when s.scorewoscm is not NULL then s.scorewoscm
                                      when avg(s.scorewoscm) over (partition by r.primaryprotein, r.country ) is not NULL
                                      then avg(s.scorewoscm) over (partition by r.primaryprotein, r.country )
                                      when avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                                      then avg(s.scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
                                 else 3.4
                                 end as scorewoscm
                                , case when s.scorewoscm is  NULL then 1 else 0 end as isscorereplace
                                , coalesce(round(s.share1,3),0) as share1
                                , coalesce(round(s.share1scm,3),0)as share1scm
                                , coalesce(v.volume_share_last,0) as volumesharelast
                                , coalesce(v.volume_share_2_last,0) as volumeshare2last
                                , p.skucode
                                , p.skuname
                                , p.skucount
                                , p.seasonalityrisk
                                --,p.pickcount
                                --,p.singlepick
                                --,coalesce(com.risk_index,1) as riskindex
                                --,coalesce(com.nr_skus,5.5) as nrskus
                                , dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
                                , TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
                           from materialized_views.int_scm_analytics_remps_recipe as r
                           left join last_recipe r2
                               on r2.unique_recipe_code=r.uniquerecipecode
                           left join last_cost rc
                               on rc.id=r2.recipe__recipe_cost
                           left join last_nutrition n
                               on n.id=r.nutritionalinfo2p
                           left join last_nutrition_cat ncat on ncat.nutritional_info__Recipe=r2.id
                           left join last_category cat on cat.id=ncat.nutritional_info__recipe_category
                           left join scores s on s.mainrecipecode=r.mainrecipecode and s.country=r.country
                           left join picklists p on p.uniquerecipecode=r.uniquerecipecode
                           left join preference as pf on pf.uniquerecipecode=r.uniquerecipecode
                           left join hqtag as ht on ht.uniquerecipecode=r.uniquerecipecode
                           left join tag as rt on rt.uniquerecipecode=r.uniquerecipecode
                           left join producttype as pt on pt.uniquerecipecode=r.uniquerecipecode
                           left join weeks as w on w.hellofresh_week=r.absolutelastused
                           left join uploads.dach_goat_risk_complexity com on com.uniquerecipecode=r.uniquerecipecode
                           left join volumes v on v.code=r.mainrecipecode
                           where lower (r.status) in ('ready for menu planning', 'pool', 'rework')
                               and case when lower (r.status) in ('ready for menu planning', 'rework') then versionused is not NULL else TRUE end
                               and rc.cost_2p >1.5
                               and rc.cost_3p>0
                               and rc.cost_4p>0
                               and lower (r.title) not like '%not use%'
                               and lower (r.title) not like '%wrong%'
                               and length (primaryprotein)>0
                               and primaryprotein <>'N/A'
                               and r.country='DACH'
                               and right (r.uniquerecipecode, 2)<>'CH'
                               and r.uniquerecipecode not like 'TEST%'
                               and r.uniquerecipecode not like 'HE%'
                               and r.uniquerecipecode not like 'ADD%'
                               and r.uniquerecipecode not like 'CO%'
                               and r.uniquerecipecode not like 'XMAS%'
                               and r.title not like 'PLACEH%'
                               and case when lower (r.status) in ('ready for menu planning') then r.absolutelastused >='2019-W01'
                                       else TRUE end
                               and ncat.primary_protein is not NULL) temp
                     where o = 1)

----- CA GAMP Recipe Pool Query -----

, scores_CA as(
select region as country,mainrecipecode
    ,sum(score*rating_count)/sum(rating_count) as scorewoscm
    ,sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    ,sum(count_of_1s)/sum(rating_count) as share1
    ,sum(count_of_1s_wscm)/sum(rating_count_wscm) as share1scm
    from(
select *
    ,dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
from materialized_views.gamp_recipe_scores
where region='CA' and score>0 and rating_count>50
) t where o=1
group by 1,2
)

,volumes_CA as(
select code, round(avg(last_region_share),4) as volume_share_last, round(avg(last_2_region_share),4) as volume_share_2_last, sum (last_count) as last_count, sum(last_2_count) as last_2_count
from views_analysts.gamp_recipe_volumes
where region='CA'
group by 1
)
-------
------ UPDATE THE PLANNING INTERVAL !!!!!!!
-------
,seasonality_CA as(
select sku,max(seasonality_score) as seasonality_score from uploads.gp_sku_seasonality where country='CA' and week>='W05' and week<='W08'
group by 1
)

, last_recipe_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipes
where remps_instance='CA'
)t where o=1
)
, last_cost_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipecost
where remps_instance='CA'
)t where o=1
)

, last_nutrition_CA AS (
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        FROM remps.recipe_nutritionalinfopp
        WHERE remps_instance='CA'
    ) AS t
    WHERE o = 1)

, last_tag_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_tags
where remps_instance='CA'
)t where o=1
)

, last_tag_map_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_tags_recipes
where remps_instance='CA'
)t where o=1
)


, last_product_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_producttypes
where remps_instance='CA'
)t where o=1
)

, last_preference_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_recipepreferences
where remps_instance='CA'
)t where o=1
)

, last_preference_map_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_recipepreferences_recipes
where remps_instance='CA'
)t where o=1
)

, last_hqtag_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_hqtags
where remps_instance='CA'
)t where o=1
)

, last_hqtag_map_CA as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_hqtags_recipes
where remps_instance='CA'
)t where o=1
)

,last_ingredient_group_CA as(
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_ingredientgroup
    WHERE remps_instance='CA'
    ) AS t
WHERE o = 1)

, last_recipe_sku_CA as(
SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_recipeskus
    WHERE remps_instance='CA'
    ) AS t
WHERE o = 1)

,last_sku_CA as(
SELECT *
FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.sku_sku
    WHERE remps_instance='CA'
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
        from last_recipe_CA r
        join last_ingredient_group_CA ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku_CA rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku_CA sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality_CA s on s.sku=sku.code
        where  rs.quantity_to_order_2p>0
        group by 1,2,3,4) t
    group by 1
)

,hqtag_CA as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.original_name,','),'') as name from last_recipe_CA rr
left join last_hqtag_map_CA m on rr.id= m.recipe_recipes_id
left join last_hqtag_CA rt on rt.id=m.recipetags_hqtags_id
group by 1
)

, preference_CA as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe_CA rr
left join last_preference_map_CA m on rr.id= m.recipe_recipes_id
left join last_preference_CA rp on rp.id=m.recipetags_recipepreferences_id
group by 1
)

,producttype_CA as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe_CA rr
left join last_product_CA rp on rp.id=rr.recipe__product_type
group by 1
)
, tag_CA as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','),'')as name from last_recipe_CA rr
left join last_tag_map_CA m on rr.id= m.recipe_recipes_id
left join last_tag_CA rt on rt.id=m.recipetags_tags_id
group by 1
)
, all_recipes_CA as (select *
                     from (select r.id                                                                      as rempsid
                                , r.country
                                , r.uniquerecipecode
                                , r.mainrecipecode                                                          as code
                                , r.version
                                , r.status
                                , r.title
                                , concat(r.title, coalesce(regexp_replace(r.subtitle, '\t|\n', ''), ''),
                                         coalesce(r.primaryprotein, ''), coalesce(r.primarystarch, ''),
                                         coalesce(r.cuisine, ''), coalesce(r.dishtype, ''),
                                         coalesce(r.primaryvegetable, ''), coalesce(r.primaryfruit, ''))    as subtitle
                                --, r.lastused
                                --,r.nextused
                                , r.absolutelastused
                                , case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end     as isnewrecipe
                                , case when r.nextused is not NULL and r.lastused is NULL then 1 else 0 end as isnewscheduled
                                , r.isdefault                                                               as isdefault
                                , r.primaryprotein
                               /*,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) as mainprotein
                                 ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) as proteincut
                                 ,coalesce(r.secondaryprotein,'none') as secondaryprotein
                                 ,concat(coalesce (r.primaryprotein,''),coalesce(r.secondaryprotein,'none')) as proteins */
                                , r.primarystarch
                               /*,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') as mainstarch
                                 ,coalesce(r.secondarystarch,'none') as secondarystarch
                                 ,concat(coalesce (r.primarystarch,''),coalesce(r.secondarystarch,'none')) as starches */
                                , coalesce(r.primaryvegetable, 'none')                                      as primaryvegetable
                               /*,coalesce(TRIM(coalesce(split_part(r.primaryvegetable,'-',1),r.primaryvegetable)),'none') as mainvegetable
                                 ,concat(coalesce (r.primaryvegetable,'none'),coalesce(r.secondaryvegetable,'none'),coalesce(r.tertiaryvegetable,'none')) as vegetables
                                 ,coalesce(r.secondaryvegetable,'none') as secondaryvegetable
                                 ,coalesce(r.tertiaryvegetable,'none') as tertiaryvegetable
                                 ,coalesce(r.primarydryspice,'none') as primarydryspice
                                 ,coalesce(r.primarycheese,'none') as primarycheese
                                 ,coalesce(r.primaryfruit,'none') as primaryfruit
                                 ,coalesce(r.primarydairy,'none') as primarydairy
                                 ,coalesce(r.primaryfreshherb,'none') as primaryfreshherb
                                 ,coalesce(r.primarysauce,'none') as primarysauce
                                 ,case when n.salt is null then 0 else n.salt end as salt */
                                , case when n.kilo_calories = 0 then 999 else n.kilo_calories end           as calories
                                , r.cuisine
                                , r.dishtype
                               /* ,case when r.handsontime ="" or r.handsontime is NULL then 0
                                      when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is NULL then 0
                                      when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is not NULL then cast( left(r.handsontime,2) as float)
                                      when length (r.handsontime) <2 then 0
                                      when r.handsontime='0' then 0
                                      else cast(r.handsontime as float) end as handsontime */
                                , case
                                      when r.totaltime = "" or r.totaltime is NULL then 0
                                      when length(r.totaltime) > 3 and cast(left (r.totaltime, 2) as float) is NULL
                                          then 0
                                      when length(r.totaltime) > 3 and cast(left (r.totaltime, 2) as float) is not NULL
                                          then cast(left (r.totaltime, 2) as float)
                                      when length(r.totaltime) < 2 then 0
                                      when r.totaltime = '0' then 0
                                      else cast(r.totaltime as float) end                                   as totaltime
                                , difficultylevel                                                           as difficulty
                                --,ht.name as hqtag
                                , rt.name                                                                   as tag
                                , pf.name                                                                   as preference
                                , concat(ht.name, rt.name, pf.name)                                         as preftag
                                , pt.name                                                                   as producttype
                                --,r.author
                                , round(rc.cost_1p, 2)                                                      as cost1p
                                , round(rc.cost_2p, 2)                                                      as cost2p
                                , round(rc.cost_3p, 2)                                                      as cost3p
                                , round(rc.cost_4p, 2)                                                      as cost4p
                                , case
                                      when scorescm is not NULL then scorescm
                                      when avg(scorescm) over (partition by r.primaryprotein, r.country ) is not NULL
                                      THEN avg(scorescm) over (partition by r.primaryprotein, r.country)
                                      when avg(scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                                      THEN avg(scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
                                      else 3.4
                                  end as scorescm
                                ,case when scorewoscm is not NULL then scorewoscm
                                      when avg(scorewoscm) over (partition by r.primaryprotein, r.country ) is not NULL
                                      THEN avg(scorewoscm) over (partition by r.primaryprotein, r.country)
                                      when avg(scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                                      THEN avg(scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
                                      else 3.4
                                 end as scorewoscm
                                ,case when s.scorewoscm is  NULL then 1 else 0 end as isscorereplace
                                ,coalesce(round(s.share1,3),0) as share1
                                ,coalesce(round(s.share1scm,3),0)as share1scm
                                ,coalesce(round(v.volume_share_last,3),0) as volumesharelast
                                ,coalesce(round(v.volume_share_2_last,3),0) as volumeshare2last
                                ,p.skucode
                                ,p.skuname
                                ,p.skucount
                                ,p.seasonalityrisk
                                ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
                                ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
                           from materialized_views.int_scm_analytics_remps_recipe as r
                           left join last_recipe_CA r2
                               on r2.unique_recipe_code=r.uniquerecipecode
                           left join last_cost_CA rc on rc.recipe_cost__recipe=r2.id
                           left join last_nutrition_CA n on n.id=r.nutritionalinfo2p
                           left join scores_CA s on s.mainrecipecode=r.mainrecipecode and s.country=r.country
                           left join picklists_CA p on p.uniquerecipecode=r.uniquerecipecode
                           left join preference_CA as pf on pf.uniquerecipecode=r.uniquerecipecode
                           left join hqtag_CA as ht on ht.uniquerecipecode=r.uniquerecipecode
                           left join tag_CA as rt on rt.uniquerecipecode=r.uniquerecipecode
                           left join producttype_CA as pt on pt.uniquerecipecode=r.uniquerecipecode
                           left join volumes_CA v on v.code=r.mainrecipecode
                           where lower (r.status) in ('ready for menu planning', 'active', 'in development')
                             and lower (r.title) not like '%not use%'
                             and lower (r.title) not like '%wrong%'
                             and lower (r.title) not like '%test%'
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
                             and r.country='CA'
                             and rc.cost_2p>0
                             and rc.recipe_cost__distribution_centre=118219490218475521) temp
                     where o = 1
                       AND scorewoscm > 0)

select * from all_recipes_DACH
UNION
select * from all_recipes_CA

/*
-------------------------------------------------------------------------------
TAXA
-------------------------------------------------------------------------------

Taking iNaturalist DDL from
https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql

Integrating data from the Catalog of Life to create titles and tags.

Integrating data from iNaturalist taxonomy table with Catalog of Life name data.

Title:
    - If the scientific name has one or more English vernacular names, collect them into
      the title, separated by commas.
    - If not, there are a few taxa that are very common where I googled an English name
      and added it manually, so use that.
    - Otherwise, use the iNaturalist name. There are a few where the name is "Not
      assigned" but we're going to filter those records out & drop associated photos.

Tags:
    - If the title of a specific taxa is a vernacular name, put the iNaturalist name in
      the tags.
    - If there are non-English vernacular names, put them in the tags.
    - Put the titles of ancestors in the tags.
    - Given the order of types of tags above plus alphabetical order, take only the
      first 20 tags.

Representing tags in this way to be consistent with python processing `_enrich_tags`:
TO DO #902: Find a DRYer way to do this enrichment with SQL
*/

/*
                   ********** Create tag type ***********
This at least makes the structure of tags a little more explicit in sql
*/
DO $$ BEGIN
    create type openverse_tag as (name varchar(255), provider varchar(255));
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

/*              ********** Load raw iNaturalist Data *********                */
DROP TABLE IF EXISTS inaturalist.taxa;
CREATE TABLE inaturalist.taxa (
    taxon_id integer,
    ancestry character varying(255),
    rank_level double precision,
    rank character varying(255),
    name character varying(255),
    active boolean
);
SELECT aws_s3.table_import_from_s3('inaturalist.taxa',
    'taxon_id, ancestry, rank_level, rank, name, active',
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')',
    'inaturalist-open-data',
    'taxa.csv.gz',
    'us-east-1');
ALTER TABLE inaturalist.taxa ADD PRIMARY KEY (taxon_id);
COMMIT;

/*
                ********** Integrate Catalog of Life Data *********

+ aggregate the catalog of life vernacular names table to the scientific name level,
  with string lists of English names and JSON for tags in other languages
+ add in information from the manual table of common names and from inaturalist taxa,
  so that every taxon record has the best possible title
+ add in inaturalist taxa base data, with the category of "Life" excluded from
  ancestry strings
*/
drop table if exists inaturalist.taxa_with_vernacular;
create table inaturalist.taxa_with_vernacular as
(
    with
    catalog_of_life_names as
    (
        select cast(md5(n.scientificname) as uuid) as md5_scientificname,
            v.name_language,
            v.taxon_name,
            row_number() OVER (partition by n.scientificname
                order by case when v.name_language='eng' then 0 else 1 end,
                v.taxon_name)
                as name_rank,
            count(case when v.name_language='eng' then 1 end)
                OVER (partition by n.scientificname
                rows between unbounded preceding and unbounded following)
                as english_names
        FROM inaturalist.col_name_usage n
            INNER JOIN inaturalist.col_vernacular v on v.taxonid = n.id
        where length(n.id) <= 10
    ),
    catalog_of_life as
    (
        SELECT
            md5_scientificname,
            string_agg(DISTINCT
                case when name_language = 'eng' then taxon_name end,
                ', ') name_english,
            jsonb_agg(DISTINCT cast((taxon_name, 'inaturalist') as openverse_tag))
                FILTER (where name_rank > english_names) as tags_nonenglish_vernacular
        FROM catalog_of_life_names
        where name_rank <= 20
        group by 1
    )
    select
        taxa.taxon_id,
        (case when ancestry='48460' then '' else replace(taxa.ancestry,'48460/','') end)
            as ancestry, --exclude 'Life' from ancestry
        left(coalesce(
            catalog_of_life.name_english,
            manual_name_additions.vernacular_name, --name_manual,
            taxa.name -- name_inaturalist,
        ), 5000) as title,
        (case when catalog_of_life.name_english is not null or
            manual_name_additions.vernacular_name is not null
            then to_jsonb(array_fill(cast((taxa.name, 'inaturalist') as openverse_tag), array[1]))
            end) inaturalist_name_tag,
        tags_nonenglish_vernacular
    from inaturalist.taxa
    LEFT JOIN catalog_of_life
        on (cast(md5(taxa.name) as uuid) = catalog_of_life.md5_scientificname)
    LEFT JOIN inaturalist.manual_name_additions
        on (cast(md5(taxa.name) as uuid) = manual_name_additions.md5_scientificname)
    where taxa.name <> 'Not assigned'
);
ALTER TABLE inaturalist.taxa_with_vernacular ADD PRIMARY KEY (taxon_id);
COMMIT;

/*
           ********** Create enriched table with ancestry tags *********

Join each record to all of its ancestor records and aggregate ancestor titles into the
tags along with json strings from the enriched data above.
    + expand each ancestry string into an array
    + get the taxa record for each value of the array
    + aggregate back to the original taxon level, with tags for ancestor names (titles)
*/
DROP table if exists inaturalist.taxa_enriched;
create table inaturalist.taxa_enriched as
(
    select
        child.taxon_id,
        child.title,
        jsonb_path_query_array(
            (
                coalesce(child.inaturalist_name_tag, to_jsonb(array[]::openverse_tag[]))
                || coalesce(jsonb_agg(DISTINCT
                    cast((ancestors.title,'inaturalist') as openverse_tag))
                    FILTER (where ancestors.title is not null),
                    to_jsonb(array[]::openverse_tag[]))
                || coalesce(child.tags_nonenglish_vernacular,
                    to_jsonb(array[]::openverse_tag[]))
            ),
            '$[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]')
        as tags
    from
        inaturalist.taxa_with_vernacular child
        left join inaturalist.taxa_with_vernacular ancestors
        on (ancestors.taxon_id = ANY (string_to_array(child.ancestry, '/')::int[]))
    group by child.taxon_id, child.title,
        child.inaturalist_name_tag, child.tags_nonenglish_vernacular
);
ALTER TABLE inaturalist.taxa_enriched ADD PRIMARY KEY (taxon_id);
COMMIT;

SELECT count(*) FROM inaturalist.taxa_enriched;

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
    - If there are non-English vernacular names, put them in the tags.
    - Put the titles of ancestors in the tags.
    - If the title of a specific taxa is a vernacular name, put the iNaturalist name in
      the tags.

Representing tags in this way to be consistent with python processing
"_enrich_tags":
https://github.com/WordPress/openverse-catalog/blob/337ea7aede228609cbd5031e3a501f22b6ccc482/openverse_catalog/dags/common/storage/media.py#L265
TO DO: Find a DRYer way to do this enrichment with SQL (see issue #902)
*/

/* ********** Load raw iNaturalist Data ********* */
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
  with string lists of English names and JSON ready strings for tags in other
  languages
+ add in information from the manual table of common names and from inaturalist taxa,
  so that every taxon record has the best possible title

*/
drop table if exists inaturalist.taxa_with_vernacular;
create table inaturalist.taxa_with_vernacular as
(
    with
    catalog_of_life as
    (
        SELECT
            cast(md5(n.scientificname) as uuid) as md5_scientificname,
            string_agg(DISTINCT
                case
                when v.name_language = 'eng' then v.taxon_name
                end,
                ', ') name_english,
            '{"name": "'||string_agg(DISTINCT
                case
                when v.name_language <> 'eng' then v.taxon_name
                end,
                '", "provider": "inaturalist"}, {"name": "')
            ||', "provider": "inaturalist"}' tags_nonenglish_vernacular
        FROM inaturalist.col_name_usage n
            INNER JOIN inaturalist.col_vernacular v on v.taxonid = n.id
        where length(n.id) <= 10
        group by 1
    )
    select
        taxa.taxon_id,
        taxa.ancestry,
        left(coalesce(
            catalog_of_life.name_english,
            manual_name_additions.vernacular_name, --name_manual,
            taxa.name -- name_inaturalist,
        ), 5000) as title,
        (case when catalog_of_life.name_english is not null
            or manual_name_additions.vernacular_name is not null
            then '{"name": "'||taxa.name||'", "provider": "inaturalist"}'
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
tags along with json-ready strings from the enriched data above.
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
        to_jsonb('['
        || (case when child.inaturalist_name_tag is not null
                then child.inaturalist_name_tag||', ' else '' end)
        || (case when child.tags_nonenglish_vernacular is not null then
        '{"name": "'||child.tags_nonenglish_vernacular||'", "provider": "inaturalist"}, '
        else '' end)
        || '{"name": "'
            ||(string_agg(DISTINCT ancestors.title,
            '", "provider": "inaturalist"}, {"name": "')
            FILTER (where ancestors.title <> 'Life'))
            ||'", "provider": "inaturalist"}'
        || ']') as tags
    from
        inaturalist.taxa_with_vernacular child,
        inaturalist.taxa_with_vernacular ancestors
    where ancestors.taxon_id = ANY (string_to_array(child.ancestry, '/')::int[])
    group by child.taxon_id, child.title,
        child.inaturalist_name_tag, child.tags_nonenglish_vernacular
);
ALTER TABLE inaturalist.taxa_enriched ADD PRIMARY KEY (taxon_id);
COMMIT;

SELECT count(*) FROM inaturalist.taxa_enriched;

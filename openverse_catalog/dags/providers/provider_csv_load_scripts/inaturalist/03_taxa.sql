/* 
********************************************************************************
TAXA 
********************************************************************************

Taking DDL from https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql
Plus adding a field for ancestry tags.
*/

DROP TABLE IF EXISTS inaturalist.taxa;
commit;

CREATE TABLE inaturalist.taxa (
    taxon_id integer,
    ancestry character varying(255),
    rank_level double precision,
    rank character varying(255),
    name character varying(255),
    active boolean,
    tags text
);

-- Load from S3
select aws_s3.table_import_from_s3('inaturalist.taxa', 
    'taxon_id, ancestry, rank_level, rank, name, active', 
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')', 
    'inaturalist-open-data', 
    'taxa.csv.gz', 
    'us-east-1');

-- doing this after the load to help performance, but will need a way to 
-- handle non-uniqueness if it comes up
ALTER TABLE inaturalist.taxa ADD PRIMARY KEY (taxon_id);

-- Aggregate ancestry names as tags
create temporary table unnest_ancestry as
(
    SELECT 
        unnest(string_to_array(ancestry, '/'))::int as linked_taxon_id, 
        taxon_id 
    FROM inaturalist.taxa
);

create temporary table taxa_tags as
(
    select u.taxon_id, STRING_AGG(taxa.name, '; ') as tags 
    from unnest_ancestry as u 
        join inaturalist.taxa on u.linked_taxon_id = taxa.taxon_id
    where taxa.rank not in ('kingdom', 'stateofmatter')
    group by u.taxon_id
);

update inaturalist.taxa
set tags = taxa_tags.tags
from inaturalist.taxa_tags
where taxa_tags.taxon_id = taxa.taxon_id;

select count(*) from inaturalist.taxa;
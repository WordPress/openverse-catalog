/*
Content Provider:       Inaturalist

ETL Process:            Use inaturalist open data on S3 to access required metadata for a full initialization load.

Output:                 Populated, indexed tables and a logical view that can be used to populate an openverse tsv file.

Notes:                  The inaturalist API is not intended for data scraping.
                        https://api.inaturalist.org/v1/docs/
                        But there is a full dump intended for sharing on S3.
                        https://github.com/inaturalist/inaturalist-open-data/tree/documentation/Metadata

TO DO:                  Consider adding a pre-processing step to get filesize, create, and modified dates for photo files 
                        stored on S3, to allow for incremental updates, and to identify and mark deleted photos.
                        https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/s3/index.html#airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator
                        Consider using this (GBIF Backbone Taxonomy dataset) to get additional names and tags for animals:
                        https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c
                        Consider how often we will need to update the taxa table and the best way to check for updates
                        Consider best way to check for license version updates
*/

-- Better to use temporary tables? Thinking of this as a first initialization supporting future incremental updates for now.
CREATE SCHEMA IF NOT EXISTS inaturalist ;

/* 
********************************************************************************
OBSERVATIONS
********************************************************************************
--  ~400,000 observations do not have a taxon_id that is in the taxa table. 
--  Their photos are not included in the final transformed view on the assumption 
    that photos are not useful to us without a title or tags
--  TO DO? consider dropping them here instead
*/

CREATE TABLE IF NOT EXISTS inaturalist.observations (
    observation_uuid uuid primary key deferrable initially deferred,
    observer_id integer,
    latitude numeric(15,10),
    longitude numeric(15,10),
    positional_accuracy integer,
    taxon_id integer,
    quality_grade character varying(255),
    observed_on date
);

select aws_s3.table_import_from_s3('inaturalist.observations', 
    '', 
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')', 
    'inaturalist-open-data', 
    'observations.csv.gz', 
    'us-east-1');

/* 
********************************************************************************
PHOTOS 
********************************************************************************
--  not using an FK constraint on observer_id to save load time, but the 5/30/2022 dataset does have complete observer ids
--  not using an FK constraint on observation_uuid to save load time, but the 5/30/2022 dataset does have complete observer ids
--  photo_id is not unique. There are ~130,000 photo_ids that appear more than once, maybe because an earlier version of 
    the photo was deleted (?) unclear, but for now I'm going to assume that they will be taken care of later in the processing 
    of these data. It does mean that we can't add an index and things will go slower when selecting on photo id. 
--  TO DO? depending on how we handle native s3 metadata, might want to drop them here
*/

CREATE TABLE IF NOT EXISTS inaturalist.photos (
    photo_uuid uuid NOT NULL,
    photo_id integer NOT NULL,
    observation_uuid uuid NOT NULL,
    observer_id integer,
    extension character varying(5),
    license character varying(255),
    width smallint,
    height smallint,
    position smallint
);

select aws_s3.table_import_from_s3('inaturalist.photos', 
    '', 
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')', 
    'inaturalist-open-data', 
    'photos.csv.gz', 
    'us-east-1');

/* 
********************************************************************************
TAXA 
********************************************************************************
*/

CREATE TABLE IF NOT EXISTS inaturalist.taxa (
    taxon_id integer primary key deferrable initially deferred,
    ancestry character varying(255),
    rank_level double precision,
    rank character varying(255),
    name character varying(255),
    active boolean,
    tags text
);

select aws_s3.table_import_from_s3('inaturalist.taxa', 
    'taxon_id, ancestry, rank_level, rank, name, active', 
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')', 
    'inaturalist-open-data', 
    'taxa.csv.gz', 
    'us-east-1');

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

/* 
********************************************************************************
OBSERVERS 
********************************************************************************
*/

CREATE TABLE IF NOT EXISTS inaturalist.observers (
    observer_id integer primary key deferrable initially deferred,
    login character varying(255),
    name character varying(255)
);

select aws_s3.table_import_from_s3('inaturalist.observers', 
    '', 
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')', 
    'inaturalist-open-data', 
    'observers.csv.gz', 
    'us-east-1');

/*
********************************************************************************
Final transformed view
********************************************************************************
*/

CREATE OR REPLACE VIEW inaturalist.tsv as
(
    select 
        p.photo_id as foreign_identifier,
        lower(p.extension) as filetype,
        lower(replace(p.license,'CC-','')) license_name,
        /*
        taking the hard coded license versions from inaturalist
        https://github.com/inaturalist/inaturalist/blob/d338ba76d82af83d8ad0107563015364a101568c/app/models/shared/license_module.rb#L5
        */
        (case 
            when p.license='CC-BY-NC-SA' then '4.0'
            when p.license='CC-BY-NC' then '4.0'
            when p.license='CC-BY-NC-ND' then '4.0'
            when p.license='CC-BY' then '4.0'
            when p.license='CC-BY-SA' then '4.0'
            when p.license='CC-BY-ND' then '4.0'
            when p.license='CC0' then '1.0'
        end) license_version,
        (CASE
            when p.license='CC-BY-NC-SA' then 'http://creativecommons.org/licenses/by-nc-sa/4.0/'
            when p.license='CC-BY-NC' then 'http://creativecommons.org/licenses/by-nc/4.0/'
            when p.license='CC-BY-NC-ND' then 'http://creativecommons.org/licenses/by-nc-nd/4.0/'
            when p.license='CC-BY' then 'http://creativecommons.org/licenses/by/4.0/'
            when p.license='CC-BY-SA' then 'http://creativecommons.org/licenses/by-sa/4.0/'
            when p.license='CC-BY-ND' then 'http://creativecommons.org/licenses/by-nd/4.0/'
            when p.license='PD' then 'http://en.wikipedia.org/wiki/Public_domain'
            when p.license='CC0' then 'http://creativecommons.org/publicdomain/zero/1.0/'
        end) license_url,
        p.width,
        p.height,
        'https://www.inaturalist.org/photos/'||p.photo_id as foreign_landing_url,
        'https://inaturalist-open-data.s3.amazonaws.com/photos/'||p.photo_id||'/medium.'||p.extension as image_url,
        coalesce(c.login, p.observer_id::text) as creator,
        'https://www.inaturalist.org/users/'||p.observer_id as creator_url,
        t.name as title,
        t.tags
    from inaturalist.photos as p
        join inaturalist.observations as o on p.observation_uuid = o.observation_uuid
        join inaturalist.observers as c on p.observer_id = c.observer_id
        join inaturalist.taxa as t on o.taxon_id = t.taxon_id
);

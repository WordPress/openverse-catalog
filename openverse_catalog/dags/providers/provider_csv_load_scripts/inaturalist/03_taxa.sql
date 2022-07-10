/*
-------------------------------------------------------------------------------
TAXA
-------------------------------------------------------------------------------

Taking DDL from
https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql
Plus adding a field for ancestry tags.
*/

DROP TABLE IF EXISTS inaturalist.taxa;
COMMIT;

CREATE TABLE inaturalist.taxa (
    taxon_id integer,
    ancestry character varying(255),
    rank_level double precision,
    rank character varying(255),
    name character varying(255),
    active boolean,
    tags text
);
COMMIT;

SELECT aws_s3.table_import_from_s3('inaturalist.taxa',
    'taxon_id, ancestry, rank_level, rank, name, active',
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')',
    'inaturalist-open-data',
    'taxa.csv.gz',
    'us-east-1');

ALTER TABLE inaturalist.taxa ADD PRIMARY KEY (taxon_id);
COMMIT;

-- Aggregate ancestry names as tags
CREATE TEMPORARY TABLE unnest_ancestry AS
(
    SELECT
        taxon_id,
        unnest(string_to_array(ancestry, '/'))::int AS linked_taxon_id
    FROM inaturalist.taxa
);

CREATE TEMPORARY TABLE taxa_tags AS
(
    SELECT
        unnest_ancestry.taxon_id,
        string_agg(taxa.name, '; ') AS tags
    FROM unnest_ancestry
    INNER JOIN
        inaturalist.taxa ON unnest_ancestry.linked_taxon_id = taxa.taxon_id
    WHERE taxa.rank NOT IN ('kingdom', 'stateofmatter')
    GROUP BY unnest_ancestry.taxon_id
);

UPDATE inaturalist.taxa
SET tags = taxa_tags.tags
FROM taxa_tags
WHERE taxa_tags.taxon_id = taxa.taxon_id;
COMMIT;

SELECT count(*) FROM inaturalist.taxa;

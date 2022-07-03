/*
********************************************************************************
OBSERVERS
********************************************************************************

Taking DDL from https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql
*/

DROP TABLE IF EXISTS inaturalist.observers;
commit;

CREATE TABLE inaturalist.observers (
    observer_id integer,
    login character varying(255),
    name character varying(255)
);

select aws_s3.table_import_from_s3('inaturalist.observers',
    '',
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')',
    'inaturalist-open-data',
    'observers.csv.gz',
    'us-east-1');

-- doing this after the load to speed performance, but will need a way to handle
-- non-uniqueness
ALTER TABLE inaturalist.observers ADD PRIMARY KEY (observer_id);

select count(*) from inaturalist.observers;

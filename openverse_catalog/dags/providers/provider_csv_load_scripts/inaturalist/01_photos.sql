/*
-------------------------------------------------------------------------------
PHOTOS
-------------------------------------------------------------------------------
--  not using an FK constraint on observer_id to save load time, but the
    5/30/2022 dataset does have complete observer ids
--  not using an FK constraint on observation_uuid to save load time, but the
    5/30/2022 dataset does have complete observer ids
--  photo_id is not unique. There are ~130,000 photo_ids that appear more than
    once, maybe because an earlier version of the photo was deleted (unclear),
    but for now assuming that will be taken care of later in the processing.

Taking DDL from
https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql
*/

DROP TABLE IF EXISTS inaturalist.photos CASCADE;
COMMIT;

CREATE TABLE inaturalist.photos (
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
COMMIT;

SELECT aws_s3.table_import_from_s3('inaturalist.photos',
    '',
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')',
    'inaturalist-open-data',
    'photos.csv.gz',
    'us-east-1');

SELECT count(*) FROM inaturalist.photos;

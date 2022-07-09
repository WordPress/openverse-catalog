/*
********************************************************************************
PHOTOS
********************************************************************************
--  not using an FK constraint on observer_id to save load time, but the 5/30/2022 dataset does have complete observer ids
--  not using an FK constraint on observation_uuid to save load time, but the 5/30/2022 dataset does have complete observer ids
--  photo_id is not unique. There are ~130,000 photo_ids that appear more than once, maybe because an earlier version of
    the photo was deleted (?) unclear, but for now I'm going to assume that they will be taken care of later in the processing
    of these data. It does mean that we can't add an index and things will go slower when selecting on photo id.
    The documentation suggests indexing on photo UUID, but the AWS files of the actual photos, are stored under photo_id

Taking DDL from https://github.com/inaturalist/inaturalist-open-data/blob/main/Metadata/structure.sql
*/

DROP TABLE IF EXISTS inaturalist.photos CASCADE;
commit;

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
commit;

select aws_s3.table_import_from_s3('inaturalist.photos',
    '',
    '(FORMAT ''csv'', DELIMITER E''\t'', HEADER, QUOTE E''\b'')',
    'inaturalist-open-data',
    'photos.csv.gz',
    'us-east-1');

select count(*) from inaturalist.photos;

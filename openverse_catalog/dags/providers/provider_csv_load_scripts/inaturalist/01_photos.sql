/*
-------------------------------------------------------------------------------
PHOTOS
-------------------------------------------------------------------------------
--  despite the 2022-05-30 data set having complete observer IDs, we do not use an FK constraint on observer_id in order to save load time
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

-- Not unique, because photo id isn't unique, and it will take too long to check.
-- btree because that is the only one that will support limit/offset without sorting.
-- more here: https://www.postgresql.org/docs/current/indexes-ordering.html
CREATE INDEX ON INATURALIST.PHOTOS USING btree (PHOTO_ID);

SELECT count(*) FROM inaturalist.photos;

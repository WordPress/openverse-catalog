/*
-------------------------------------------------------------------------------
Load Intermediate Table
-------------------------------------------------------------------------------

    ** Please note: This SQL will not run as is! You must replace offset_num and
    batch_limit with integers representing the records you want to retrieve.

Joining two very large normalized tables is difficult, in any data manipulation system.
PHOTOS has on the order of 120 million records, and OBSERVATIONS has on the order of
70 million records. We have to join them to get at the taxa (species) information for
any given photo. Taxa are the only descriptive text we have for inaturalist photos.

This file uses database pagination instead of limit/offset: This would have avoided the
need to sort / index, but might introduce data quality risks (if postgres moved
things around while the job was running) and some pages appear empty which requires
more complicated python logic for retries. More on this approach at:
https://www.citusdata.com/blog/2016/03/30/five-ways-to-paginate/

Everything on iNaturalist is holding at version 4, except CC0 which is version 1.0.
License versions below are hard-coded from inaturalist
https://github.com/inaturalist/inaturalist/blob/d338ba76d82af83d8ad0107563015364a101568c/app/models/shared/license_module.rb#L5

Using image columns version 001 from common.storage.tsv_columns.
*/

INSERT INTO {intermediate_table}
(
    SELECT
        INATURALIST.PHOTOS.PHOTO_ID as FOREIGN_ID,
        'https://www.inaturalist.org/photos/' || INATURALIST.PHOTOS.PHOTO_ID
            as LANDING_URL,
        'https://inaturalist-open-data.s3.amazonaws.com/photos/'
        || INATURALIST.PHOTOS.PHOTO_ID || '/medium.' || INATURALIST.PHOTOS.EXTENSION
            as DIRECT_URL,
        null::varchar(10) as THUMBNAIL,
        -- only jpg, jpeg, png & gif in 6/2022 data, all in extensions.py for images
        lower(INATURALIST.PHOTOS.EXTENSION) as FILETYPE,
        null::int as FILESIZE,
        INATURALIST.LICENSE_CODES.OPENVERSE_CODE as LICENSE,
        INATURALIST.LICENSE_CODES.LICENSE_VERSION,
        COALESCE(INATURALIST.OBSERVERS.LOGIN, INATURALIST.PHOTOS.OBSERVER_ID::text)
            as CREATOR,
        'https://www.inaturalist.org/users/' || INATURALIST.PHOTOS.OBSERVER_ID
            as CREATOR_URL,
        left(string_agg(INATURALIST.TAXA.NAME, ' & '), 5000) as TITLE,
        -- TO DO: should there be a timestamp or anything in the metadata or is null ok?
        null::json as META_DATA,
        -- TO DO: confirm format here, string list format? json?
        string_to_array(string_agg(INATURALIST.TAXA.ancestor_names,'|'),'|') as TAGS,
        'photograph' as CATEGORY,
        null::boolean as WATERMARKED,
        'inaturalist' as PROVIDER,
        'inaturalist' as SOURCE,
        'provider_api' as INGESTION_TYPE,
        INATURALIST.PHOTOS.WIDTH,
        INATURALIST.PHOTOS.HEIGHT
    FROM INATURALIST.PHOTOS
    INNER JOIN
        INATURALIST.OBSERVATIONS ON
            INATURALIST.PHOTOS.OBSERVATION_UUID = INATURALIST.OBSERVATIONS.OBSERVATION_UUID
    INNER JOIN
        INATURALIST.OBSERVERS ON
            INATURALIST.PHOTOS.OBSERVER_ID = INATURALIST.OBSERVERS.OBSERVER_ID
    INNER JOIN
        INATURALIST.TAXA ON
            INATURALIST.OBSERVATIONS.TAXON_ID = INATURALIST.TAXA.TAXON_ID
    INNER JOIN
        INATURALIST.LICENSE_CODES ON
            INATURALIST.PHOTOS.LICENSE = INATURALIST.LICENSE_CODES.INATURALIST_CODE
    WHERE INATURALIST.PHOTOS.PHOTO_ID BETWEEN {page_start} AND {page_end}
    GROUP BY
        INATURALIST.PHOTOS.PHOTO_ID,
        INATURALIST.PHOTOS.WIDTH,
        INATURALIST.PHOTOS.HEIGHT,
        lower(INATURALIST.PHOTOS.EXTENSION),
        INATURALIST.LICENSE_CODES.OPENVERSE_CODE,
        INATURALIST.LICENSE_CODES.LICENSE_VERSION,
        COALESCE(INATURALIST.OBSERVERS.LOGIN, INATURALIST.PHOTOS.OBSERVER_ID::text),
        INATURALIST.PHOTOS.OBSERVER_ID
)
;
COMMIT;

SELECT count(*) records
FROM {intermediate_table} ;

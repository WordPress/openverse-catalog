/*
-------------------------------------------------------------------------------
Build Transformed Table
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

TO DO:
- integrate back with the regular dag steps starting with the cleaning and reporting
  step at the end of common.sql.load_s3_data_to_intermediate_table
- handle duplicate photo IDs (though many fewer now than before, may need to aggregate
  observers)
*/

INSERT INTO INATURALIST.TRANSFORMED
(
    SELECT
        INATURALIST.PHOTOS.PHOTO_ID as foreign_identifier,
        INATURALIST.PHOTOS.WIDTH,
        INATURALIST.PHOTOS.HEIGHT,
        left(string_agg(INATURALIST.TAXA.NAME, ' & '), 5000) as title,
        string_to_array(string_agg(INATURALIST.TAXA.ancestor_names,'|'),'|') as raw_tags,
        -- only jpg, jpeg, png, and gif in June 2022 data, all in extensions.py for images
        lower(INATURALIST.PHOTOS.EXTENSION) as filetype,
        INATURALIST.LICENSE_CODES.OPENVERSE_CODE AS LICENSE,
        INATURALIST.LICENSE_CODES.LICENSE_VERSION,
        COALESCE(INATURALIST.OBSERVERS.LOGIN, INATURALIST.PHOTOS.OBSERVER_ID::text)
            as creator,
        'https://www.inaturalist.org/users/' || INATURALIST.PHOTOS.OBSERVER_ID
            as creator_url,
        now() as ingestion_timestamp,
        -- going for consistency with prod code here see provider_details.py
        'provider_api' as ingestion_type,
        'inaturalist' as provider, -- seems like provider and source are the same
        'photograph' as category,
        'https://www.inaturalist.org/photos/' || INATURALIST.PHOTOS.PHOTO_ID
            as foreign_landing_url,
        'https://inaturalist-open-data.s3.amazonaws.com/photos/'
        || INATURALIST.PHOTOS.PHOTO_ID || '/medium.' || INATURALIST.PHOTOS.EXTENSION
            as image_url
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
    WHERE photo_id between {page_start} and {page_end}
    GROUP BY INATURALIST.PHOTOS.PHOTO_ID,
        INATURALIST.PHOTOS.WIDTH,
        INATURALIST.PHOTOS.HEIGHT,
        INATURALIST.PHOTOS.EXTENSION,
        INATURALIST.LICENSE_CODES.OPENVERSE_CODE,
        INATURALIST.LICENSE_CODES.LICENSE_VERSION,
        COALESCE(INATURALIST.OBSERVERS.LOGIN, INATURALIST.PHOTOS.OBSERVER_ID::text),
        'https://www.inaturalist.org/users/' || INATURALIST.PHOTOS.OBSERVER_ID
)
;
COMMIT;

SELECT count(*) records
FROM INATURALIST.TRANSFORMED;

/*
-------------------------------------------------------------------------------
EXPORT DB PAGE TO JSON
-------------------------------------------------------------------------------

Please note: This SQL will not run as is! You must replace db_page_number with
an integer representing the page you want to retrieve.

This takes a single page of data from inaturalist.photos, finds matching
records from observers, observations, and taxa, and returns a json for
further processing.
*/

WITH
PHOTO_PAGE AS (
    SELECT
        PHOTO_ID,
        EXTENSION,
        LICENSE,
        WIDTH,
        HEIGHT,
        OBSERVER_ID,
        OBSERVATION_UUID
    FROM INATURALIST.PHOTOS
    -- lifted from
    -- https://www.citusdata.com/blog/2016/03/30/five-ways-to-paginate/
    WHERE CTID = ANY(ARRAY(
            SELECT ('(db_page_number,' || S.I || ')')::tid
            FROM
                GENERATE_SERIES(
                    DB_PAGE_NUMBER, CURRENT_SETTING('block_size')::int / 4
                )
                AS S(I)
        )
    )
),

TRANSFORMED AS (
    SELECT
        PHOTO_PAGE.PHOTO_ID AS FOREIGN_ID,
        PHOTO_PAGE.WIDTH,
        /*
        Taking the hard coded license versions from inaturalist
        https://github.com/inaturalist/inaturalist/blob/d338ba76d82af83d8ad0107563015364a101568c/app/models/shared/license_module.rb#L5
        Everything is holding at version 4, except CC0 which is version 1.0.
        */
        PHOTO_PAGE.HEIGHT,
        INATURALIST.TAXA.NAME AS TITLE,
        INATURALIST.TAXA.TAGS,
        LOWER(PHOTO_PAGE.EXTENSION) AS FILETYPE,
        (CASE
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY-NC-SA'
                THEN 'http://creativecommons.org/licenses/by-nc-sa/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY-NC'
                THEN 'http://creativecommons.org/licenses/by-nc/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY-NC-ND'
                THEN 'http://creativecommons.org/licenses/by-nc-nd/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY'
                THEN 'http://creativecommons.org/licenses/by/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY-SA'
                THEN 'http://creativecommons.org/licenses/by-sa/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'CC-BY-ND'
                THEN 'http://creativecommons.org/licenses/by-nd/4.0/'
            WHEN PHOTO_PAGE.LICENSE = 'PD'
                THEN 'http://en.wikipedia.org/wiki/Public_domain'
            WHEN PHOTO_PAGE.LICENSE = 'CC0'
                THEN 'http://creativecommons.org/publicdomain/zero/1.0/'
            END) AS LICENSE_URL,
        'https://www.inaturalist.org/photos/' || PHOTO_PAGE.PHOTO_ID
        AS FOREIGN_LANDING_URL,
        'https://inaturalist-open-data.s3.amazonaws.com/photos/'
        || PHOTO_PAGE.PHOTO_ID || '/medium.' || PHOTO_PAGE.EXTENSION
        AS IMAGE_URL,
        COALESCE(
            INATURALIST.OBSERVERS.LOGIN, PHOTO_PAGE.OBSERVER_ID::text
        ) AS CREATOR,
        'https://www.inaturalist.org/users/' || PHOTO_PAGE.OBSERVER_ID
        AS CREATOR_URL
    FROM PHOTO_PAGE
    INNER JOIN
        INATURALIST.OBSERVATIONS ON
            PHOTO_PAGE.OBSERVATION_UUID = INATURALIST.OBSERVATIONS.OBSERVATION_UUID
    INNER JOIN
        INATURALIST.OBSERVERS ON
            PHOTO_PAGE.OBSERVER_ID = INATURALIST.OBSERVERS.OBSERVER_ID
    INNER JOIN
        INATURALIST.TAXA ON
            INATURALIST.OBSERVATIONS.TAXON_ID = INATURALIST.TAXA.TAXON_ID
)

SELECT ROW_TO_JSON(TRANSFORMED.*) AS RESPONSE_JSON
FROM TRANSFORMED;

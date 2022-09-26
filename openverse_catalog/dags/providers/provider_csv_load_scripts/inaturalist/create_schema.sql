CREATE SCHEMA IF NOT EXISTS inaturalist;
COMMIT;
SELECT schema_name
FROM information_schema.schemata WHERE schema_name = 'inaturalist';

/* target staging table */

DROP TABLE IF EXISTS inaturalist.transformed;
COMMIT;

CREATE TABLE inaturalist.transformed (
    foreign_identifier integer,
    width smallint,
    height smallint,
    title varchar(5000),
    raw_tags varchar[],
    filetype varchar(5),
    license varchar(50),
    license_version varchar(25),
    creator varchar(2000),
    creator_url varchar(2000),
    ingestion_timestamp timestamp,
    ingestion_type varchar(80),
    provider varchar(80),
    category varchar(80),
    foreign_landing_url varchar(1000),
    image_url varchar(3000)
);
COMMIT;

/* license look-up for cleaning */

DROP TABLE IF EXISTS inaturalist.license_codes;
COMMIT;

CREATE TABLE inaturalist.license_codes (
    inaturalist_code varchar(50),
    license_name varchar(255),
    license_url varchar(255),
    openverse_code varchar(50),
    license_version varchar(25)
);
COMMIT;

INSERT INTO inaturalist.license_codes
    (inaturalist_code, license_name, license_url, openverse_code, license_version)
    VALUES
    ('CC_BY_NC_SA', 'Creative Commons Attribution-NonCommercial-ShareAlike License', 'http://creativecommons.org/licenses/by-nc-sa/4.0/', 'by-nc-sa', '4.0'),
    ('CC_BY_NC', 'Creative Commons Attribution-NonCommercial License', 'http://creativecommons.org/licenses/by-nc/4.0/', 'by-nc', '4.0'),
    ('CC_BY_NC_ND', 'Creative Commons Attribution-NonCommercial-NoDerivs License', 'http://creativecommons.org/licenses/by-nc-nd/4.0/', 'by-nc-nd', '4.0'),
    ('CC_BY', 'Creative Commons Attribution License', 'http://creativecommons.org/licenses/by/4.0/', 'by', '4.0'),
    ('CC_BY_SA', 'Creative Commons Attribution-ShareAlike License', 'http://creativecommons.org/licenses/by-sa/4.0/', 'by-sa', '4.0'),
    ('CC_BY_ND', 'Creative Commons Attribution-NoDerivs License', 'http://creativecommons.org/licenses/by-nd/4.0/', 'by-nd', '4.0'),
    ('PD', 'Public domain', 'http://en.wikipedia.org/wiki/Public_domain', 'pdm', ''),
    ('GFDL', 'GNU Free Documentation License', 'http://www.gnu.org/copyleft/fdl.html', 'gfdl', ''),
    ('CC0', 'Creative Commons CC0 Universal Public Domain Dedication', 'http://creativecommons.org/publicdomain/zero/1.0/', 'cc0', '1.0');
COMMIT;

-- /* considered only loading selection from observations, but didn't save much time */

-- DROP TABLE IF EXISTS inaturalist.observations;
-- COMMIT;

-- CREATE TABLE inaturalist.observations (
--     observation_uuid uuid,
--     taxon_id integer
-- );
-- COMMIT;

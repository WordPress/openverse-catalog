CREATE SCHEMA IF NOT EXISTS inaturalist;
COMMIT;
SELECT schema_name
FROM information_schema.schemata WHERE schema_name = 'inaturalist';

/*
LICENSE LOOKUP
Everything on iNaturalist is holding at version 4, except CC0 which is version 1.0.
License versions below are hard-coded from inaturalist
https://github.com/inaturalist/inaturalist/blob/d338ba76d82af83d8ad0107563015364a101568c/app/models/shared/license_module.rb#L5
*/

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
    ('CC-BY-NC-SA', 'Creative Commons Attribution-NonCommercial-ShareAlike License', 'http://creativecommons.org/licenses/by-nc-sa/4.0/', 'by-nc-sa', '4.0'),
    ('CC-BY-NC', 'Creative Commons Attribution-NonCommercial License', 'http://creativecommons.org/licenses/by-nc/4.0/', 'by-nc', '4.0'),
    ('CC-BY-NC-ND', 'Creative Commons Attribution-NonCommercial-NoDerivs License', 'http://creativecommons.org/licenses/by-nc-nd/4.0/', 'by-nc-nd', '4.0'),
    ('CC-BY', 'Creative Commons Attribution License', 'http://creativecommons.org/licenses/by/4.0/', 'by', '4.0'),
    ('CC-BY-SA', 'Creative Commons Attribution-ShareAlike License', 'http://creativecommons.org/licenses/by-sa/4.0/', 'by-sa', '4.0'),
    ('CC-BY-ND', 'Creative Commons Attribution-NoDerivs License', 'http://creativecommons.org/licenses/by-nd/4.0/', 'by-nd', '4.0'),
    ('PD', 'Public domain', 'http://en.wikipedia.org/wiki/Public_domain', 'pdm', ''),
    ('GFDL', 'GNU Free Documentation License', 'http://www.gnu.org/copyleft/fdl.html', 'gfdl', ''),
    ('CC0', 'Creative Commons CC0 Universal Public Domain Dedication', 'http://creativecommons.org/publicdomain/zero/1.0/', 'cc0', '1.0');
COMMIT;

/*
********************************************************************************
Final transformed view
********************************************************************************

This is really just a placeholder for the transformation logic in SQL. I'm not at
all sure that a massive logical view is an implementation that makes any sense at
all for actually updating the images table.
*/

CREATE OR REPLACE VIEW inaturalist.tsv as
(
    select
        p.photo_id as foreign_identifier,
        lower(p.extension) as filetype,
        lower(replace(p.license,'CC-','')) license_name,
        /*
        taking the hard coded license versions from inaturalist
        https://github.com/inaturalist/inaturalist/blob/d338ba76d82af83d8ad0107563015364a101568c/app/models/shared/license_module.rb#L5
        */
        (case
            when p.license='CC-BY-NC-SA' then '4.0'
            when p.license='CC-BY-NC' then '4.0'
            when p.license='CC-BY-NC-ND' then '4.0'
            when p.license='CC-BY' then '4.0'
            when p.license='CC-BY-SA' then '4.0'
            when p.license='CC-BY-ND' then '4.0'
            when p.license='CC0' then '1.0'
        end) license_version,
        (CASE
            when p.license='CC-BY-NC-SA' then 'http://creativecommons.org/licenses/by-nc-sa/4.0/'
            when p.license='CC-BY-NC' then 'http://creativecommons.org/licenses/by-nc/4.0/'
            when p.license='CC-BY-NC-ND' then 'http://creativecommons.org/licenses/by-nc-nd/4.0/'
            when p.license='CC-BY' then 'http://creativecommons.org/licenses/by/4.0/'
            when p.license='CC-BY-SA' then 'http://creativecommons.org/licenses/by-sa/4.0/'
            when p.license='CC-BY-ND' then 'http://creativecommons.org/licenses/by-nd/4.0/'
            when p.license='PD' then 'http://en.wikipedia.org/wiki/Public_domain'
            when p.license='CC0' then 'http://creativecommons.org/publicdomain/zero/1.0/'
        end) license_url,
        p.width,
        p.height,
        'https://www.inaturalist.org/photos/'||p.photo_id as foreign_landing_url,
        'https://inaturalist-open-data.s3.amazonaws.com/photos/'||p.photo_id||'/medium.'||p.extension as image_url,
        coalesce(c.login, p.observer_id::text) as creator,
        'https://www.inaturalist.org/users/'||p.observer_id as creator_url,
        t.name as title,
        t.tags
    from inaturalist.photos as p
        join inaturalist.observations as o on p.observation_uuid = o.observation_uuid
        join inaturalist.observers as c on p.observer_id = c.observer_id
        join inaturalist.taxa as t on o.taxon_id = t.taxon_id
);

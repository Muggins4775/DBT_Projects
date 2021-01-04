
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (
    SELECT 	listing_id,
			run_id,
			file_date,
			data_source,
			source_type,
			url,
			breadcrumb,
			breadcrumb_uid,
			breadcrumb_matches,
			closest_mmi_uid,
			closest_km_to_match,
			match_level,
			parsed_state_alias,
			parsed_district_alias,
			parsed_subdistrict_alias,
			parsed_city_alias,
			parsed_locality_alias,
			parsed_sublocality_alias,
			parsed_village_alias,
			centroid_latitude,
			centroid_longitude,
			address,
			/* Latitude and Longitude can have some garbage information in it, so clean out only the valid ones with regexp */
			NULLIF(regexp_substr(latitude, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT, 0) AS latitude,
			NULLIF(regexp_substr(longitude, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT, 0) AS longitude,
			coord_match_country,
			coord_match_state,
			property_id,
			master_project_id,
			master_project_name,
			master_developer,
			project_name,
			listing_project_id,
			rera_id,
			building_name,
			/* Construction status is most easily found by looking for terms and keywords */
			CASE
				WHEN (construction_status IN ('ready', 'completed') OR construction_status LIKE '%ready%') THEN 'ready'
				WHEN (construction_status IN ('under construction','not done', 'ongoing') OR construction_status LIKE '%under construction%') THEN 'under construction'
				/* Some datasets contain construction status as a date, so convert it to date and infer whether that date is before or after the scrape date and assign status accordingly. If it's before scrape date, it's assumed ready. */
				WHEN REGEXP_LIKE(construction_status, '[0-9]{2} [a-z]{3} [0-9]{4}') THEN IFF(TO_DATE(REGEXP_SUBSTR(construction_status, '[0-9]{2} [a-z]{3} [0-9]{4}', 1, 1, 'i'), 'DD MON YYYY') < FILE_DATE, 'ready', 'under construction')
			END AS construction_status,
			CASE 
				WHEN sale_type LIKE '%new%' THEN 'new sale'
				WHEN sale_type LIKE '%resale%' THEN 'resale'
			END AS sale_type,
			/* Only look for these specific words. If more are found, amend this list */
			REGEXP_SUBSTR(property_type, 'apartment|house|villa|plot|floor', 1, 1, 'i') AS property_type,
			developer,
			/* When a year can be identified in this field through regexp, use it. If construction status contains a date, use that as year built */
			CASE 
				WHEN year_built IS NOT NULL THEN regexp_substr(year_built, '[0-9]{4}')::INTEGER 
				WHEN REGEXP_LIKE(construction_status, '[0-9]{2} [a-z]{3} [0-9]{4}') THEN EXTRACT(YEAR FROM TO_DATE(REGEXP_SUBSTR(construction_status, '[0-9]{2} [a-z]{3} [0-9]{4}', 1, 1, 'i'), 'DD MON YYYY'))
			END AS year_built,
			age_of_property,
			bedrooms,
			bathrooms,
			REGEXP_SUBSTR(bedrooms, '[0-9]')::INTEGER AS bedrooms_parsed,
			REGEXP_SUBSTR(bathrooms, '[0-9]')::INTEGER AS bathrooms_parsed,
			carpet_area,
			/*
			 * All area variables are parsed in the same way. If a listing is of type property, it is assumed to only have a lower figure.
			 * If it is of type project or configuration, an attempt to extract both a lower and upper range is made. Area figures are 
			 * assumed to be in square metres, though units are parsed anyway for the fun of it.
			 */
			REGEXP_SUBSTR(carpet_area, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT AS carpet_area_lower,
			IFF(source_type != 'property', REGEXP_SUBSTR(carpet_area, '([0-9]+\\.?[0-9]+?)+', 1, 2, 'i')::FLOAT, NULL) AS carpet_area_upper,
			REGEXP_SUBSTR(carpet_area, '([^0-9])+$', 1, 1, 'i') AS carpet_area_units,
			built_up_area,
			REGEXP_SUBSTR(built_up_area, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT AS built_up_area_lower,
			IFF(source_type != 'property', REGEXP_SUBSTR(built_up_area, '([0-9]+\\.?[0-9]+?)+', 1, 2, 'i')::FLOAT, NULL) AS built_up_area_upper,
			REGEXP_SUBSTR(built_up_area, '([^0-9])+$', 1, 1, 'i') AS built_up_area_units,
			super_built_up_area,
			REGEXP_SUBSTR(super_built_up_area, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT AS super_built_up_area_lower,
			IFF(source_type != 'property', REGEXP_SUBSTR(super_built_up_area, '([0-9]+\\.?[0-9]+?)+', 1, 2, 'i')::FLOAT, NULL) AS super_built_up_area_upper,
			REGEXP_SUBSTR(super_built_up_area, '([^0-9])+$', 1, 1, 'i') AS super_built_up_area_units,
			/*
			 * Price variables are assumed to have only a lower figure if property, and a range if project or config. It is not assumed that
			 * the lower figure and upper figure are in the same units, so units must be parsed out for both.
			 */
			price_total,
			REGEXP_SUBSTR(price_total, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT AS price_total_lower,
			IFF(source_type != 'property', REGEXP_SUBSTR(price_total, '([0-9]+\\.?[0-9]+?)+', 1, 2, 'i')::FLOAT, NULL) AS price_total_upper,
			UPPER(REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(price_total), '([0-9]+\\.?[0-9]?+ ?[L|C|K|R]?)', 1, 1, 'i'), '[A-Z]', 1, 1,'i')) AS price_total_lower_units,
			IFF(source_Type != 'property', UPPER(REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(price_total), '([0-9]+\\.?[0-9]?+ ?[L|C|K|R]?)', 1, 2, 'i'), '[A-Z]', 1, 1,'i')), NULL) AS price_total_upper_units,
			price_per_unit,
			REGEXP_SUBSTR(price_per_unit, '([0-9]+\\.?[0-9]+?)+', 1, 1, 'i')::FLOAT AS price_per_unit_lower,
			IFF(source_type != 'property', REGEXP_SUBSTR(price_per_unit, '([0-9]+\\.?[0-9]+?)+', 1, 2, 'i')::FLOAT, NULL) AS price_per_unit_upper,
			UPPER(REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(price_per_unit), '([0-9]+\\.?[0-9]?+ ?[L|C|K|R]?)', 1, 1, 'i'), '[A-Z]', 1, 1,'i')) AS price_per_unit_lower_units,
			IFF(source_Type != 'property', UPPER(REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(price_per_unit), '([0-9]+\\.?[0-9]?+ ?[L|C|K|R]?)', 1, 2, 'i'), '[A-Z]', 1, 1,'i')), NULL) AS price_per_unit_upper_units,
			possession_date,
			update_date
	FROM {{ ref('joined_listings') }} listings
)

select *
from source_data

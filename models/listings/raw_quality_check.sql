
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (
    SELECT 	listing_id,
			run.id as RUN_ID,
			run.file_date::DATE AS file_date,
			run.data_source,
			run.source_type,
			url,
			breadcrumb,
			breadcrumb_uid::TEXT AS breadcrumb_uid,
			REGEXP_COUNT(breadcrumb_uid::TEXT,';')+1 AS breadcrumb_matches,
			COALESCE(closest_mmi_uid, IFF(CHARINDEX(';', breadcrumb_uid) > 0, NULL, breadcrumb_uid))::NUMERIC AS closest_mmi_uid,
			closest_km_to_match,
			address,
			dataset.latitude,
			dataset.longitude,
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
			lower(construction_status) AS construction_status_raw,
       
			/* Some age of property records actually contain construction status information, so extract it */
			CASE 	WHEN lower(age_of_property) = 'under construction' THEN 'under construction'
					WHEN lower(age_of_property) = 'new construction' THEN 'ready'
					ELSE lower(construction_status)
			END AS construction_status,
       
			/* The housing datasource actually contains in the URL whether a listing is a resale, so it must be parsed out */
			CASE WHEN data_source = 'housing' THEN lower(split_part(url, '/', 6)) ELSE lower(SALE_TYPE) END AS sale_type,
				
			/* In some datasets, the property type is contained in the URL so parse it out if property_type field is unavailable */
			COALESCE(lower(property_type), lower(split_part(url, '.com/', 2))) AS property_type,
			developer,
				
			/* Where the year_built is of the form MMM-YYYY, extract and convert it to date */
			TO_DATE('01-' || REGEXP_SUBSTR(year_built, '[A-Z]{3}-[0-9]{4}', 1, 1, 'i'), 'DD-MON-YYYY') AS year_built,
				
			/* Where the age of property field contains numbers, extract the last occurring number from it. This captures the last value if it's a range */
			REGEXP_SUBSTR(age_of_property, '[0-9]+',1, GREATEST(1, REGEXP_COUNT(age_of_property, '[0-9]+',1,'i')))::INTEGER AS age_of_property,
				
			/* Some datasets contain the bedroom information in other fields, and it must be parsed out. This is pretty much universally identified by looking for BHK */
			CASE 	WHEN bedrooms IS NOT NULL THEN bedrooms
					WHEN data_source = 'makaan' AND source_type = 'property' THEN regexp_substr(url, '[0-9]bhk+', 1, 1, 'i')
					WHEN bedrooms IS NULL AND property_type IS NOT NULL THEN regexp_substr(property_type, '([0-9]{1} ?[BHK]+)', 1, 1, 'i')
			END AS bedrooms,
			bathrooms,
				
			/* Remove commas from price and area features to make regexp parsing easier */
			REPLACE(carpet_area, ',', '') AS carpet_area,
				
			/* Makaan actually contains the built up area information in the URL and it must be parsed out */
			IFF(data_source = 'makaan' AND source_type = 'property', REGEXP_SUBSTR(url, '[0-9]+\.?(sqft)', 1, 1, 'i'), REPLACE(built_up_area, ',', '')) AS built_up_area,
			REPLACE(super_built_up_area, ',', '') AS super_built_up_area,
			REPLACE(price_total, ',', '') AS price_total,
			REPLACE(price_per_unit, ',', '') AS price_per_unit,
			CASE 	WHEN regexp_like(possession_date, '^([0-9]{4}-[0-9]{2}-[0-9]{2})$') THEN TO_DATE(possession_date, 'YYYY-MM-DD')
					WHEN regexp_like(possession_date, '^([A-Za-z]{3} [0-9]{4})$') THEN TO_DATE(CONCAT('01 ', possession_date), 'DD MON YYYY')
					WHEN regexp_like(possession_date, '^([A-Za-z]{3}''[0-9]{2})$') THEN TO_DATE(CONCAT('01 ', possession_date), 'DD MON''YY')
					ELSE NULL
			END AS possession_date,
			CASE 	WHEN regexp_like(update_date, '^([A-Za-z]{3} [0-9]{2}, [0-9]{4})$') THEN TO_DATE(update_date, 'MON DD, YYYY')
					WHEN regexp_like(update_date, '^([A-Za-z]{3} [0-9]{1}, [0-9]{4})$') THEN TO_DATE(update_date, 'MON DD, YYYY')
					WHEN regexp_like(update_date, '^([A-Za-z]{3} [0-9]{2}, ''[0-9]{2})$') THEN TO_DATE(update_date, 'MON DD, ''YY')
					WHEN regexp_like(update_date, '^([0-9]{2}-[A-Za-z]{3}-[0-9]{2})$') THEN TO_DATE(update_date, 'DD-MON-YY')
					WHEN regexp_like(update_date, '^([0-9]{2}-[A-Za-z]{3}-[0-9]{4})$') THEN TO_DATE(update_date, 'DD-MON-YYYY')
					ELSE NULL
			END AS update_date,
			CASE WHEN closest_mmi_uid IS NOT NULL THEN closest_mmi_uid::NUMERIC
                  WHEN closest_mmi_uid IS NULL AND CHARINDEX(';', breadcrumb_uid) = 0 THEN breadcrumb_uid::NUMERIC
                  WHEN closest_mmi_uid IS NULL AND CHARINDEX(';', breadcrumb_uid) > 0 THEN -1
			 END as join_uid
		FROM PUBLIC.LISTINGS_RAW dataset
		INNER JOIN PUBLIC.LISTINGS_QUALITY_CHECK_RUN run 
		ON dataset.id = run.id
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

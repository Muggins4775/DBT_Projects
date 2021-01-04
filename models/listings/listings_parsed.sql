
{{ config(materialized='table', transient=false) }}

with source_data as (
    SELECT  listing_id,
		run_id,                                                                                                                                                                          
		file_date,                                                                                                                                                                   
		data_source,                                                                                                                                                                 
		source_type,                                  
		/* Some URL are not actually URL and should be removed */
		IFF(LEFT(url, 4) = 'http', url, NULL) AS url,
		/* Mark a URL as the latest if it is the most recently scraped version, or mark a listing as the latest if it has no valid URL */
		IFF(LEFT(COALESCE(url, 'missing'), 4) != 'http' OR row_number() over(PARTITION BY COALESCE(url, 'missing'), data_source, source_type ORDER BY file_date desc) = 1, 1, 0) AS latest_flag,		
		breadcrumb,
		breadcrumb_uid,
		breadcrumb_matches,
		closest_mmi_uid,
		closest_km_to_match,
		match_level,
		/* The first element of an alias string from MMI is assumed to be the 'common name' to be carried forward */
		SPLIT_PART(parsed_state_alias, ';', 1) AS parsed_state,
		SPLIT_PART(parsed_district_alias, ';', 1) AS parsed_district,
		SPLIT_PART(parsed_subdistrict_alias, ';', 1) AS parsed_subdistrict,
		SPLIT_PART(parsed_city_alias, ';', 1) AS parsed_city,
		SPLIT_PART(parsed_locality_alias, ';', 1) AS parsed_locality,
		SPLIT_PART(parsed_sublocality_alias, ';', 1) AS parsed_sublocality,
		SPLIT_PART(parsed_village_alias, ';', 1) AS parsed_village,
		parsed_state_alias,
		parsed_district_alias,
		parsed_subdistrict_alias,
		parsed_city_alias,
		parsed_locality_alias,
		parsed_sublocality_alias,
		parsed_village_alias,
		address,
		/* If the breadcrumb is not null and match level is null, that means multiple matches were found for the breadrumb */
		listings.latitude AS listing_latitude,
		listings.longitude AS listing_longitude,
		centroid_latitude,                                                                                               
		centroid_longitude, 
		/* Distance between the longitude and latitude of the listing and the centroid of the location found through breadcrumb parsing */
		HAVERSINE(listings.latitude, listings.longitude, centroid_latitude, centroid_longitude) AS haversine_dist,
		coord_match_country,
		coord_match_state,
		/* 
		 * Technically, there should only be one URL at a particular longitude latitude within a data source, source type, and scrape date. THere should also only
		 * be one project name at a particular longitue latitude. These flags signal when that does not hold. 
		 */
		IFF(count(DISTINCT url) OVER(PARTITION BY listing_longitude, listing_latitude, data_source, source_type, file_date) > 1, 0, 1) AS coord_url_duplicate_ok,
		count(DISTINCT url) OVER(PARTITION BY listing_longitude, listing_latitude, data_source, source_type, file_date) AS coord_url_count,
		IFF(count(DISTINCT project_name) OVER(PARTITION BY listing_longitude, listing_latitude, data_source, source_type, file_date) > 1, 0, 1) AS coord_project_duplicate_ok,
		count(DISTINCT project_name) OVER(PARTITION BY listing_longitude, listing_latitude, data_source, source_type, file_date) AS coord_project_count,
		property_id,
		master_project_id,
		master_project_name,
		master_developer,
		project_name,    
		listing_project_id,
		rera_id,                                                                                                                                                                     
		building_name,
		/* When construction status is not available, infer it from other fields */
		CASE 
			WHEN construction_status IS NOT NULL THEN construction_status
			WHEN sale_type = 'resale' THEN 'ready'
			WHEN year_built >= 1900 AND year_built < extract(YEAR FROM file_date) THEN 'ready'
			WHEN age_of_property <= 100 AND age_of_property > 0 THEN 'ready'
		END AS construction_status,
		possession_date,
		sale_type,                                                                                                              
		property_type,                                                                                
		developer,
		/* When year built is not available and age of property is, infer year built */
		CASE 
			WHEN year_built >= 1900 THEN year_built
			WHEN age_of_property <= 100 THEN EXTRACT(YEAR FROM file_date)-age_of_property
		END AS year_built,
		/* When age of property is not available and year built is, infer age of property */
		CASE
			WHEN age_of_property <= 100 THEN age_of_property
			WHEN year_built >= 1900 THEN EXTRACT(YEAR FROM file_date)-year_built
		END AS age_of_property,
		bedrooms,                                                                                                                  
		bathrooms,
		bedrooms_parsed,
		bathrooms_parsed,
		carpet_area,                                                                                                                                                                 
		carpet_area_lower,                                                                                   
		carpet_area_upper,                                             
		built_up_area,                                                                                                                                                               
		built_up_area_lower,                                                                               
		built_up_area_upper,                                         
		super_built_up_area,                                                                                                                                                         
		super_built_up_area_lower,                                                                   
		super_built_up_area_upper,                             
		price_total,
		/*
		 * Based on the first letter of the parsed units, convert the value to rupees
		 * R = Rupees
		 * K = thousands
		 * L = Lakh
		 * C = Crore
		 * If no units found, do not make an assumption unless value > 10000 then assume rupees
		 */
		price_total_lower * CASE
			WHEN LEFT(price_total_lower_units, 1) = 'R' THEN 1
			WHEN LEFT(price_total_lower_units, 1) = 'K' THEN 1e3
			WHEN LEFT(price_total_lower_units, 1) = 'L' THEN 1e5
			WHEN LEFT(price_total_lower_units, 1) = 'C' THEN 1e7
			WHEN LEFT(price_total_lower_units, 1) IS NULL AND price_total_lower >= 1e5 THEN 1
		END AS price_total_lower,                                                                                   
		price_total_upper * CASE
			WHEN LEFT(price_total_upper_units, 1) = 'R' THEN 1
			WHEN LEFT(price_total_upper_units, 1) = 'K' THEN 1e3
			WHEN LEFT(price_total_upper_units, 1) = 'L' THEN 1e5
			WHEN LEFT(price_total_upper_units, 1) = 'C' THEN 1e7
			WHEN LEFT(price_total_upper_units, 1) IS NULL AND price_total_lower >= 1e5 THEN 1
		END AS price_total_upper,
		price_per_unit,
		price_per_unit_lower * CASE
			WHEN LEFT(price_per_unit_lower_units, 1) = 'K' THEN 1e3
			WHEN LEFT(price_per_unit_lower_units, 1) = 'L' THEN 1e5
			WHEN LEFT(price_per_unit_lower_units, 1) = 'C' THEN 1e7
			ELSE 1
		END AS price_per_unit_lower,                                                                                   
		price_per_unit_upper * CASE
			WHEN LEFT(price_per_unit_upper_units, 1) = 'K' THEN 1e3
			WHEN LEFT(price_per_unit_upper_units, 1) = 'L' THEN 1e5
			WHEN LEFT(price_per_unit_upper_units, 1) = 'C' THEN 1e7
			ELSE 1
		END AS price_per_unit_upper,
		update_date,
		api.city AS mmi_city
	FROM {{ ref('recast_listings') }} listings
	LEFT JOIN public.API_RESULTS_MMI_LATEST api 
	ON LOWER(CONCAT_WS(',', project_name, SPLIT_PART(parsed_locality_alias, ';', 1), SPLIT_PART(parsed_city_alias, ';', 1), SPLIT_PART(parsed_state_alias, ';', 1))) = api.query
	
)

select *
from source_data

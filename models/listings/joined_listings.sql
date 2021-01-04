
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (
    
		SELECT  rqc.listing_id,
				rqc.run_id,
				rqc.file_date,
				rqc.data_source,
				rqc.source_type,
				rqc.url,
				rqc.breadcrumb,
       
				CASE WHEN mmi.match_level IS NOT NULL THEN mmi.match_level
					 WHEN mmi.match_level IS NULL AND breadcrumb_uid IS NOT NULL THEN 'multi match'
					 WHEN mmi.match_level IS NULL AND breadcrumb_uid IS NULL AND address IS NOT NULL AND source_type = 'property' AND data_source = 'magicbriks' THEN 'no match'
					 WHEN mmi.match_level IS NULL AND breadcrumb_uid IS NULL AND breadcrumb IS NOT NULL THEN 'no match'
				END AS match_level,
       
				rqc.breadcrumb_uid,
				rqc.breadcrumb_matches,
				rqc.closest_mmi_uid,
				rqc.closest_km_to_match,
       
				mmi.state AS parsed_state_alias,
				mmi.district AS parsed_district_alias,
				mmi.subdistrict AS parsed_subdistrict_alias,
				mmi.city AS parsed_city_alias,
				IFF(match_level IN ('locality','sublocality'), mmi.locality, NULL) AS parsed_locality_alias,
				IFF(match_level IN ('sublocality'), mmi.sublocality, NULL) AS parsed_sublocality_alias,
				IFF(match_level IN ('village'), mmi.village, NULL) AS parsed_village_alias,
				mmi.latitude AS centroid_latitude,
				mmi.longitude AS centroid_longitude,
       
				rqc.address,
				rqc.latitude,
				rqc.longitude,
				rqc.coord_match_country,
				rqc.coord_match_state,
				rqc.property_id,
				rqc.master_project_id,
				rqc.master_project_name,
				rqc.master_developer,
				rqc.project_name,
				rqc.listing_project_id,
				rqc.rera_id,
				rqc.building_name,
				rqc.construction_status_raw,
				rqc.construction_status,
				rqc.sale_type,
				rqc.property_type,
				rqc.developer,
				rqc.year_built,
				rqc.age_of_property,
				rqc.bedrooms,
				rqc.bathrooms,
				rqc.carpet_area,
				rqc.built_up_area,
				rqc.super_built_up_area,
				rqc.price_total,
				rqc.price_per_unit,
				rqc.possession_date,
				rqc.update_date
		FROM {{ ref('raw_quality_check') }} rqc
		LEFT OUTER JOIN {{ ref('address_mmi_subset') }} mmi
		ON mmi.uid = rqc.join_uid
)

select *
from source_data


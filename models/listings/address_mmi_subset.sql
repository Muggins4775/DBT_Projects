
{{ config(materialized='table') }}

with source_data as (
    SELECT 	uid::NUMERIC as uid,
			state,
			district,
			subdistrict,
			city,
			locality,
			sublocality,
			subsublocality,
			road,
			village,
			pincode,
			type,
			latitude,
			longitude,
			CASE WHEN TYPE = 4 THEN 'sublocality'
				 WHEN TYPE = 5 THEN 'locality'
				 WHEN TYPE = 6 THEN 'city'
				 WHEN TYPE = 7 THEN 'village'
			END AS match_level
    FROM PUBLIC.ADDRESSMMI
)

select *
from source_data

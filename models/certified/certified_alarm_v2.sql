{{
    config(
        schema="csm_certified",
        materialized="incremental",
        unique_key=["asset_id", "alarm_id", "activated_at"],
    )
}}

select 
    alert_id,
    asset_id,
    alarm_id,
    activated_at,
    deactivated_at,
    duration_sec,
    ingested_at,
    activated_gps_lat,
    activated_gps_lon,
    name,
    oem_code,
    severity,
    organization_name,
    site_name,
    asset_serial,
    asset_name,
    asset_type,
    asset_model,
    current_timestamp as csm_insert_date_timestamp
FROM {{ ref('landing_alarm_v2') }}
{% if is_incremental() %}
  WHERE activated_at >= dateadd(day, -7, current_date)
  AND activated_at > (SELECT MAX(activated_at) FROM {{ this }})
{% else %}
  WHERE activated_at >= '1900-01-01' 
{% endif %}

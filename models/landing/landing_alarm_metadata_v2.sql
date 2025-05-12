{{
    config(
        schema="csm_landing",
        materialized="incremental",
        unique_key=["ASSET_ID", "ALARM_ID", "ACTIVATED_AT"],
    )
}}

-- import ctes

with alarm_data as (
    select 
        alert_id,
        asset_id,
        alarm_id,
        activated_at,
        deactivated_at,
        duration_sec
    from {{ source("wenco_source", "ALARM_V2") }}
),
alarm_metadata as (
    select
        alarm_id,
        name,
        asset_name,
        asset_model
    from {{ source("internal_source", "RL_IO_ASSET_ALARM_METADATA_V1") }}
),

--logical ctes 
base as (
    select
            alarm_data.ALERT_ID,
            alarm_data.ALARM_ID,
            alarm_metadata.NAME,
            alarm_data.ASSET_ID,
            alarm_metadata.ASSET_NAME,
            alarm_metadata.ASSET_MODEL,
            alarm_data.ACTIVATED_AT,
            alarm_data.DEACTIVATED_AT,
            alarm_data.DURATION_SEC,
            current_timestamp as CSM_INSERT_DATE_TIMESTAMP
    from alarm_data
    join alarm_metadata 
        on alarm_data.ALARM_ID = alarm_metadata.ALARM_ID
    {% if is_incremental() %}
        where
            alarm_data.activated_at >= dateadd(day, -7, current_date)
            and alarm_data.activated_at > (
                select max(activated_at) 
                from {{ this }} 
                where activated_at is not null
            )
    {% else %}
        where alarm_data.activated_at >= '1900-01-01'
    {% endif %}
)
select *
from base

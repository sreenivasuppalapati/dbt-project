{{
    config(
        schema="csm_landing",
        materialized="incremental",
        unique_key=["asset_id", "alarm_id", "activated_at"],
    )
}}

-- import ctes
with
    alarm_data as (
        select * from {{ source("wenco_source", "ALARM_V2") }}
        ),
    alarm_metadata as (
        select * from {{ source("internal_source", "rl_io_asset_alarm_metadata_v1") }}
    ),

    -- logical ctes
    base as (
        select
            alarm_data.alert_id,
            alarm_metadata.name,
            alarm_data.asset_id,
            alarm_metadata.asset_name,
            alarm_metadata.asset_model,
            alarm_data.activated_at,
            alarm_data.deactivated_at,
            alarm_data.duration_sec,
            current_timestamp as csm_insert_date_timestamp
        from alarm_data
        join alarm_metadata on alarm_data.alarm_id = alarm_metadata.alarm_id
        {% if is_incremental() %}
            where
                activated_at >= dateadd(day, -7, current_date)
                and activated_at > (select max(activated_at) from {{ this }})
        {% else %} where activated_at >= '1900-01-01'
        {% endif %}
    )

select *
from base

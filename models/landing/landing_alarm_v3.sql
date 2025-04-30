{{
    config (
        tag = ["csm_landing"]
    )
}}

select * from {{ source('wenco_landing','alarm_v2')}}
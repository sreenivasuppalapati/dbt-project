{{ config(schema='CSM_SERVING') }}

select * from {{ref ('certified_alarm_v2')}}
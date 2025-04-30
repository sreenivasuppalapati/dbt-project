{{config (schema = 'csm_landing')}}


select * from {{source('wenco_source','ALARM_V2')}}
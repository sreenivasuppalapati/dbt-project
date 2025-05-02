select alert_id
    from {{ref('landing_alarm_v2')}}
where alert_id is null
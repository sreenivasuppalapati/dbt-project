{{ config(schema='CSM_SERVING') }}

select * from {{ref ('hello_python')}}
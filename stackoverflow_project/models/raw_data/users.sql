{{ config(materialized='table') }}
select * from {{ source('stackoverflow', 'users') }}
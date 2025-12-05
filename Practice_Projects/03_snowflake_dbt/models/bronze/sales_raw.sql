-- bronze: raw sales table (source)
select * from {{ source('raw','sales') }}

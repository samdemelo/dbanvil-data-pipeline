{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    on_schema_change='sync_all_columns',
    post_hook=[
        "
        delete from {{ this }} as target
        where not exists (
            select 1
            from {{ ref('stg_users') }} as source
            where source.user_id = target.user_id
        )
        "
    ]
) }}

select
    user_id,
    confirmation_sent_at,
    email_confirmed_at,
    created_at,
    last_sign_in_at,
    marketing_opt_in,
    email_domain,
    load_timestamp
from {{ ref('stg_users') }}
select
    cast(id as varchar) as user_id,
    confirmation_sent_at,
    email_confirmed_at,
    created_at,
    last_sign_in_at,
    marketing_opt_in,

    case
        when lower(trim(email_domain)) in (
            -- Google
            'gmail.com',

            -- Microsoft
            'outlook.com',
            'hotmail.com',
            'live.com',
            'msn.com',

            -- Yahoo
            'yahoo.com',
            'yahoo.ca',
            'yahoo.co.uk',

            -- Apple
            'icloud.com',
            'me.com',
            'mac.com',

            -- Proton
            'proton.me',
            'protonmail.com',

            -- Other common providers
            'aol.com',
            'mail.com',
            'gmx.com',
            'zoho.com',
            'yandex.com'
        )
        then null
        else lower(trim(email_domain))
    end as email_domain,
    load_timestamp

from {{ source('raw', 'users') }}
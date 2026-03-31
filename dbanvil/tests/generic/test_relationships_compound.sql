{% test relationships_compound(model, from_columns, to_model, to_columns) %}

with child as (
    select
        {% for col in from_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ model }}
),

parent as (
    select
        {% for col in to_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ to_model }}
),

validation_errors as (
    select
        child.*
    from child
    left join parent
        on
        {% for i in range(from_columns | length) %}
        child.{{ from_columns[i] }} = parent.{{ to_columns[i] }}
        {% if not loop.last %} and {% endif %}
        {% endfor %}
    where
        {% for col in to_columns %}
        parent.{{ col }} is null
        {% if not loop.last %} and {% endif %}
        {% endfor %}
)

select *
from validation_errors

{% endtest %}
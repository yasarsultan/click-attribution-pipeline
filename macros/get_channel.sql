{% macro get_channel() %}
case
    when lower(medium) in ('cpc', 'ppc') then 'Paid Search'
    when lower(medium) = 'organic' then 'Organic Search'
    when lower(source) in ('facebook', 'instagram', 'twitter', 'linkedin') then 'Social'
    when lower(medium) = 'email' then 'Email'
    when lower(medium) = 'referral' then 'Referral'
    when medium is null then 'Direct'
    else 'Other'
end
{% endmacro %}
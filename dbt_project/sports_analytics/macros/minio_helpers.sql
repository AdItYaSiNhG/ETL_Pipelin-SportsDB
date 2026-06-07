{% macro minio_s3_path(bucket, key_pattern) %}
    s3a://{{ bucket }}/{{ key_pattern }}
{% endmacro %}

{% macro safe_cast(column, dtype) %}
    TRY_CAST({{ column }} AS {{ dtype }})
{% endmacro %}

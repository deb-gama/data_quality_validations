count_by_column = """
    SELECT
    count({count_column}) as count,
    {agg_column} 
    FROM {table}
    {where_max_value_condition}
    GROUP BY {agg_column}
    ORDER BY {agg_column}
"""

get_max_value = """
    SELECT max({max_col}) as max from {table}
"""

schema_query = """SELECT * FROM {table} limit 1"""

check_null_limit = """SELECT count(*) as null_count from {table} where {null_column} is null"""

check_duplicates = """
    SELECT {column}, COUNT(*) 
    FROM {table} 
    GROUP BY {column} 
    {where_condition}
    HAVING COUNT(*) > 1
"""
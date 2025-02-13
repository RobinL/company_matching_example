import duckdb
import altair as alt
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
import altair as alt
import pandas as pd
import time

start_time = time.time()
# https://download.companieshouse.gov.uk/en_output.html
# https://www.data.gov.uk/dataset/f027145b-b55f-4602-b889-b28a8ca04462/stockport-council-business-rates


# Create a DuckDB connection
con = duckdb.connect()


business_rates_path = "./stockport_business_rates.csv"

con.sql(f"select * from read_csv_auto('{business_rates_path}')").show(
    max_rows=10, max_width=100000
)

sql = f"""

select distinct
    AccountName as company_name,
    null::varchar as company_number,
    concat_ws(' ', Address1, Address2, Address3, Address4, Address5) as address_concat,
    COALESCE(
        REGEXP_EXTRACT(concat_ws(' ', Address1, Address2, Address3, Address4, Address5), '(\\d+[A-Z]?)'),
        REGEXP_EXTRACT(concat_ws(' ', Address1, Address2, Address3, Address4, Address5), '(\\S+)(?=\\s+HOUSE)')
    ) AS first_num_in_address,
    PostCode as postcode,

from read_csv_auto('{business_rates_path}')
order by company_name

"""
stockport_business_rates = con.sql(sql)
stockport_business_rates.show(max_rows=50, max_width=100000)

sql = f"""
select count(*) from read_csv_auto('{business_rates_path}')
"""
con.sql(sql).show(max_width=100000, max_rows=100)


# Define the input CSV file and output Parquet file
all_companies_path = "./BasicCompanyDataAsOneFile-2025-02-01.csv"

con.sql(f"select * from read_csv_auto('{all_companies_path}')").show(
    max_rows=10, max_width=100000
)

sql = f"""
select * from read_csv_auto('{all_companies_path}')
limit 10
"""
con.sql(sql).show(max_rows=10, max_width=100000)


sql = f"""
select count(*) from read_csv_auto('{all_companies_path}')
"""
con.sql(sql).show(max_rows=10, max_width=100000)

sql = f"""
select
    CompanyName as company_name,
    CompanyNumber as company_number,
    concat_ws(' ', "RegAddress.AddressLine1", "RegAddress.AddressLine2") as address_concat,
    COALESCE(
        REGEXP_EXTRACT(concat_ws(' ', "RegAddress.AddressLine1", "RegAddress.AddressLine2"), '(\\d+[A-Z]?)'),
        REGEXP_EXTRACT(concat_ws(' ', "RegAddress.AddressLine1", "RegAddress.AddressLine2"), '(\\S+)(?=\\s+HOUSE)')
    ) AS first_num_in_address,
    "RegAddress.PostCode" as postcode,
from read_csv_auto('{all_companies_path}')
-- where substr("RegAddress.PostCode".replace(' ', ''),1,4) in (
-- select distinct substr(PostCode.replace(' ', ''),1,4) from read_csv_auto('{business_rates_path}')
-- )
"""

local_companies = con.sql(sql)
local_companies.show(max_rows=10, max_width=100000)

con.sql("select count(*) from local_companies").show()

sql = """
create or replace table all_input_data as
with concat_data as (
    select *, 'stockport' as source_dataset

from stockport_business_rates
union all
select *, 'z_all_companies' as source_dataset
from local_companies
)
select ROW_NUMBER() OVER () as unique_id, *
from concat_data
"""

con.execute(sql)

con.table("all_input_data").show(max_rows=10, max_width=100000)


sql = """
create or replace table tokenised_data as
with unnested as (
    select
        unique_id,
        unnest(regexp_split_to_array(upper(trim(company_name)), '\\s+')) as name_token,
        generate_subscripts(regexp_split_to_array(upper(trim(company_name)), '\\s+'), 1) as token_position
    from all_input_data
),
token_frequencies as (
    select
        name_token as token,
        count(*)::float/(select count(*) from unnested) as rel_freq
    from unnested
    group by token
),
tokens_with_freq AS (
    SELECT
        m.unique_id,
        list_transform(
            list_zip(
                array_agg(u.name_token ORDER BY u.token_position),
                array_agg(COALESCE(tf.rel_freq, 0.0) ORDER BY u.token_position)
            ),
            x -> struct_pack(token := x[1], rel_freq := x[2])
        ) as name_tokens_with_freq
    FROM all_input_data m
    JOIN unnested u ON m.unique_id = u.unique_id
    LEFT JOIN token_frequencies tf ON u.name_token = tf.token
    GROUP BY m.unique_id
),
tokens_unnested as (
    SELECT
        unique_id,
        unnest(name_tokens_with_freq) as token_info
    FROM tokens_with_freq
),
rare_tokens as (
    SELECT
        unique_id,
        array_agg(token_info.token ORDER BY token_info.rel_freq ASC)[:2] as rarest_tokens
    FROM tokens_unnested
    WHERE token_info.rel_freq < 0.01
    GROUP BY unique_id
)
SELECT
    m.*,
    t.name_tokens_with_freq,
    r.rarest_tokens
FROM all_input_data m
LEFT JOIN tokens_with_freq t ON m.unique_id = t.unique_id
LEFT JOIN rare_tokens r ON m.unique_id = r.unique_id
"""
con.execute(sql)
con.table("tokenised_data").show(max_rows=10, max_width=100000)

df_stockport = con.sql(
    "select * from tokenised_data where source_dataset = 'stockport'"
)
df_stockport.show(max_rows=10, max_width=100000)

df_all_companies = con.sql(
    "select * from tokenised_data where source_dataset = 'z_all_companies'"
)
df_all_companies.show(max_rows=10, max_width=100000)


def calculate_tf_product_array_sql(token_rel_freq_array_name):
    return f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                {token_rel_freq_array_name}_l,
                x -> CASE
                        WHEN array_contains(
                            list_transform({token_rel_freq_array_name}_r, y -> y.token),
                            x.token
                        )
                        THEN x.rel_freq
                        ELSE 1.0
                    END
            )
        ),
        (p, q) -> p * q
    )
    """


db_api = DuckDBAPI(connection=con)


settings = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="unique_id",
    comparisons=[
        # The 1e-10 thresholds are user configurable and the number of thresholds and the values
        # of the thresholds should be chosen by reference to the real data
        # e.g. run modls with different thresholds to see what works best for the data
        {
            "output_column_name": "name_tokens_with_freq",
            "comparison_levels": [
                {
                    "sql_condition": '"name_tokens_with_freq_l" IS NULL OR "name_tokens_with_freq_r" IS NULL',
                    "label_for_charts": "name_tokens_with_freq is NULL",
                    "is_null_level": True,
                },
                {
                    "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-12
                    """,
                    "label_for_charts": "Array product is less than 1e-10",
                },
                {
                    "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-10
                    """,
                    "label_for_charts": "Array product is less than 1e-10",
                },
                {
                    "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-8
                    """,
                    "label_for_charts": "Array product is less than 1e-8",
                },
                {
                    "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-6
                    """,
                    "label_for_charts": "Array product is less than 1e-6",
                },
                {
                    "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-4
                    """,
                    "label_for_charts": "Array product is less than 1e-6",
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
            "comparison_description": "ExactMatch",
        },
        cl.PostcodeComparison("postcode"),
        cl.ExactMatch("first_num_in_address"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("postcode"),
        block_on("rarest_tokens", "substr(postcode,1,3)"),
        "l.rarest_tokens[1] = r.rarest_tokens[2] and substr(l.company_name,1,3) = substr(r.company_name,1,3)",
        "l.rarest_tokens[2] = r.rarest_tokens[1] and substr(l.company_name,1,3) = substr(r.company_name,1,3)",
        block_on("company_name"),
    ],
    additional_columns_to_retain=["address_concat", "company_name"],
)

linker = Linker([df_stockport, df_all_companies], settings, db_api)


linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
linker.visualisations.match_weights_chart()

links = linker.inference.predict(threshold_match_weight=-5)
links.as_duckdbpyrelation().show(max_rows=10, max_width=100000)


sql = f"""
WITH RankedMatches AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY unique_id_l ORDER BY match_weight DESC) as rank
    FROM {links.physical_name}
),
best_match as (
    select * from RankedMatches
    where rank = 1
    order by match_weight desc
),
matched_stockport_ids as (
    select distinct unique_id_l
    from best_match
)
select * from best_match

order by match_probability desc
"""

ranked_matches = con.sql(sql)
ranked_matches.show(max_rows=100, max_width=100000)
ranked_matches.description

sql = f"""
WITH matched_stockport_ids as (
    select distinct unique_id_l
    from ranked_matches
)
select
    -10 as match_weight,
    0.0 as match_probability,
    'stockport' as source_dataset_l,
    NULL as source_dataset_r,
    t.unique_id as unique_id_l,
    NULL as unique_id_r,
    t.name_tokens_with_freq as name_tokens_with_freq_l,
    NULL as name_tokens_with_freq_r,
    0.0 as gamma_name_tokens_with_freq,
    t.postcode as postcode_l,
    NULL as postcode_r,
    0.0 as gamma_postcode,
    t.first_num_in_address as first_num_in_address_l,
    NULL as first_num_in_address_r,
    0.0 as gamma_first_num_in_address,
    t.company_name as company_name_l,
    NULL as company_name_r,
    t.rarest_tokens as rarest_tokens_l,
    NULL as rarest_tokens_r,
    t.address_concat as address_concat_l,
    NULL as address_concat_r,
    'unmatched' as match_key,
    1 as rank
from tokenised_data t
where t.source_dataset = 'stockport'
and t.unique_id not in (select unique_id_l from matched_stockport_ids)
"""

unmatched_records = con.sql(sql)
unmatched_records.show(max_rows=100, max_width=100000)

# Combine matched and unmatched records
sql = """
select * from ranked_matches
union all
select * from unmatched_records
order by match_probability desc
"""

all_records = con.sql(sql)
all_records.show(max_rows=100, max_width=100000)

sql = """
select * from ranked_matches
where company_name_l != company_name_r
order by random()
limit 1
"""

con.sql(sql).show(max_rows=10, max_width=100000)

sql = """
select * from ranked_matches
where postcode_l != postcode_r
order by random()
limit 1
"""

con.sql(sql).show(max_rows=10, max_width=100000)

# Create the histogram
chart = (
    alt.Chart(all_records.df())
    .mark_bar()
    .encode(
        alt.X("match_weight:Q", bin=alt.Bin(maxbins=50), title="Match Weight"),
        alt.Y("count():Q", title="Count"),
    )
    .properties(title="Distribution of Match Probabilities", width=600, height=400)
)

# Save the chart
chart

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

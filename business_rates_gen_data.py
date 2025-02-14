import duckdb
import time

# https://download.companieshouse.gov.uk/en_output.html
# https://www.data.gov.uk/dataset/f027145b-b55f-4602-b889-b28a8ca04462/stockport-council-business-rates


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
    PostCode as postcode,


from read_csv_auto('{business_rates_path}')
order by company_name
"""
stockport_business_rates = con.sql(sql)
stockport_business_rates.to_parquet("stockport_business_rates.parquet")


# Define the input CSV file and output Parquet file
all_companies_path = "./BasicCompanyDataAsOneFile-2025-02-01.csv"

con.sql(f"select * from read_csv_auto('{all_companies_path}')").show(
    max_rows=10, max_width=100000
)


sql = f"""
select
    CompanyName as company_name,
    CompanyNumber as company_number,
    concat_ws(' ', "RegAddress.AddressLine1", "RegAddress.AddressLine2") as address_concat,
    "RegAddress.PostCode" as postcode,
from read_csv_auto('{all_companies_path}')
 where substr("RegAddress.PostCode".replace(' ', ''),1,4) in (
 select distinct substr(PostCode.replace(' ', ''),1,4) from read_csv_auto('{business_rates_path}')
)
"""

companies_house = con.sql(sql)
companies_house.to_parquet("companies_house.parquet")

companies_house

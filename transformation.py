import boto3
import pandas as pd
from io import StringIO
import time

athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

Dict = {}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)
    

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
enigma_jhud = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_state_abv limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
static_datasets_state_abv = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_states_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
rearc_covid_19_testing_data_states_daily = download_and_load_query_results(athena_client, response)
rearc_covid_19_testing_data_states_daily

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_us_total_latest limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
rearc_covid_19_testing_data_us_total_latest = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_county",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
nytimes_data_in_usa_us_county = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usaus_states limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
nytimes_data_in_usaus_states = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycode limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
static_datasets_countrycode = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countypopulation limit 1000",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
static_datasets_countypopulation = download_and_load_query_results(athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_usa_hospital_beds limit 1",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {'EncryptionOption': 'SSE_S3'},
    },
)
rearc_usa_hospital_beds = download_and_load_query_results(athena_client, response)

new_header = static_datasets_state_abv.iloc[0]
static_datasets_state_abv = static_datasets_state_abv[1:]
static_datasets_state_abv.columns = new_header
static_datasets_state_abv

fact_covid_1 = enigma_jhud[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
fact_covid_2 = rearc_covid_19_testing_data_states_daily[['fips','date','positive','negative','hospitalizedcurrently']]
fact_covid = pd.merge(fact_covid_1,fact_covid_2,on='fips',how='inner')


dim_region_1 = enigma_jhud[['fips','province_state','country_region','latitude','longitude']]
dim_region_2 = nytimes_data_in_usa_us_county[['fips','county','state']]
dim_region = pd.merge(dim_region_1,dim_region_1,on='fips',how='inner')


dimDate = rearc_covid_19_testing_data_states_daily[['fips','date']]
dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek

csv_buffer = StringIO()
fact_covid.to_csv(csv_buffer)
s3_resource = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,)
s3_resource.Object(S3_BUCKET_NAME,'output/factCovid.csv').put(Body=csv_buffer.getvalue())

csv_buffer = StringIO()
dim_region.to_csv(csv_buffer)
s3_resource.Object(S3_BUCKET_NAME,'output/dimRegion.csv').put(Body=csv_buffer.getvalue())

csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_resource.Object(S3_BUCKET_NAME,'output/dimDate.csv').put(Body=csv_buffer.getvalue())

dimDateSql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDateSql))

factCovidSql = pd.io.sql.get_schema(fact_covid.reset_index(), 'FactCovid')
print(''.join(factCovidSql))

dimRegSql = pd.io.sql.get_schema(dim_region.reset_index(), 'dimRegion')
print(''.join(dimRegSql))






import redshift_connector

redshift = boto3.client('redshift',region_name='ap-south-1',aws_access_key_id=KEY, aws_secret_access_key=SECRET)
conn = redshift_connector.connect(
    host=REDSHIFT_HOST,
    database='dev',
    user='awsuser',
    password=password
 )
conn.autocommit = True

cursor = conn.cursor()
cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
"""
 )

cursor.execute("""
CREATE TABLE "FactCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL
)
"""
 )

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state_x" TEXT,
  "country_region_x" TEXT,
  "latitude_x" REAL,
  "longitude_x" REAL,
  "province_state_y" TEXT,
  "country_region_y" TEXT,
  "latitude_y" REAL,
  "longitude_y" REAL
)
"""
 )

cursor.execute("""
copy factcovid from 's3://salman-covid-de-project/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::123368361769:role/redshift-s3-access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
"""
)

cursor.execute("""
copy dimregion from 's3://salman-covid-de-project/output/dimRegion.csv'
credentials 'aws_iam_role=arn:aws:iam::123368361769:role/redshift-s3-access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
"""
)


cursor.execute("""
copy dimdate from 's3://salman-covid-de-project/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::123368361769:role/redshift-s3-access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
"""
)


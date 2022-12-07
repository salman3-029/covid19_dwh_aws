# covid19_dwh_aws
Code for the AWS Project using Amazon S3, Crawler, Athena, Glue &amp; Redshift

- Data is initially fetched from S3, read by crawler & made available on AWS Athena for querying
- Python Script querying Athena & & storing data in Pandas Dataframe & transforming it using Python, before creating a final output csv and storing it back to Amazon S3
- Python Script creating facts & dimension tables & inserting data from files stored in S3 to Redshift 
![image](https://user-images.githubusercontent.com/6167021/206229353-53f31c09-eb1b-427d-ba65-9e4d2f2bb5bd.png)

import configparser
import boto3
import psycopg2
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('./cluster.config'))

KEY=config.get('AWS','KEY')
SECRET=config.get('AWS','SECRET')
DWH_CLUSTER_TYPE=config.get('DWH','DWH_CLUSTER_TYPE')
DWH_NUM_NODES=config.get('DWH','DWH_NUM_NODES')
DWH_NODE_TYPE=config.get('DWH','DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER=config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_DB=config.get('DWH','DWH_DB')
DWH_DB_USER=config.get('DWH','DWH_DB_USER')
DWH_DB_PASSWORD=config.get('DWH','DWH_DB_PASSWORD')
DWH_PORT=config.get('DWH','DWH_PORT')
DWH_IAM_ROLE=config.get('DWH','DWH_IAM_ROLE')

ec2 = boto3.resource('ec2',region_name='ap-south-1',aws_access_key_id=KEY, aws_secret_access_key=SECRET)
s3 = boto3.resource('s3',region_name='ap-south-1',aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam',region_name='ap-south-1',aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift',region_name='ap-south-1',aws_access_key_id=KEY, aws_secret_access_key=SECRET)



bucket = s3.Bucket('salman3029-bucket')
log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='')]
log_data_files


roleArn = iam.get_role(RoleName=DWH_IAM_ROLE)['Role']['Arn']
try:
    response = redshift.create_cluster(
    ClusterType=DWH_CLUSTER_TYPE,
    NodeType=DWH_NODE_TYPE,
    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,
    
    #roles (for s3 access)
    IamRoles=[roleArn]
    )
except Exception as e:
    print(e)
    
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
DWH_ENDPOINT=myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN=myClusterProps['IamRoles'][0]['IamRoleArn']
DB_NAME=myClusterProps['DBName']
DB_USER=myClusterProps['MasterUsername']

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
    
try:
    conn=psycopg2.connect(host=DWH_ENDPOINT,dbname=DB_NAME,user=DB_USER,password='Passw0rd123', port=5439)
except psycopg2.Error as e:
    print('Err: could not make conn with pg db')
    print(e)
conn.set_session(autocommit=True)

try:
    conn.close()
except psycopg2.Error as e:
    print('Err: could not make conn with pg db')
    print(e)
    


def delete_all():
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    

import configparser
import boto3
import json

# Read config
config = configparser.ConfigParser()
config.read('dwh.cfg')
KEY = config.get('AWS', 'key')
SECRET = config.get('AWS', 'secret')
HOST = config.get('CLUSTER', 'HOST')
CLUSTER_IAM_ROLE_NAME = config.get('CLUSTER', 'IAM_ROLE_NAME')
REGION = "us-east-1"
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')
CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
NUMBER_OF_NODES = config.get('CLUSTER', 'NUMBER_OF_NODES')
NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

# iam client
iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)


def create_role() -> str:
    """
    Create IAM role for the Redshift cluster. Role name is specified by
    CLUSTER_IAM_ROLE_NAME in config.

    Returns
    -------
    str
        IAM role ARN
    """
    # Create the IAM role (if not exists)
    print('Create a new IAM Role. Allow Redshift clusters to call '
          'AWS services.')
    try:
        iam.create_role(
            Path='/',
            RoleName=CLUSTER_IAM_ROLE_NAME,
            Description="Allow Redshift clusters to call AWS services on your "
                        "behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
    # print ARN (Amazon Resource Name) of the IAM role
    iam_role = iam.get_role(RoleName=CLUSTER_IAM_ROLE_NAME)
    role_arn = iam_role['Role']['Arn']
    return role_arn


def attach_s3_read_only_access() -> None:
    """Attach AmazonS3ReadOnlyAccess to the existing IAM role.

    Role name is specified by CLUSTER_IAM_ROLE_NAME in config.
    """
    print('Attaching AmazonS3ReadOnlyAccess to IAM role')
    # Attach the "AmazonS3ReadOnlyAccess" policy to the IAM role
    # specified by CLUSTER_IAM_ROLE_NAME
    iam.attach_role_policy(RoleName=CLUSTER_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']


def create_cluster(role_arn: str) -> None:
    """Create Redshift cluster.

    Print an exception if cluster creation
    fails (e.g. if cluster already exists).

    Parameters
    ----------
    role_arn : str
        IAM role ARN
    """
    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    try:
        redshift.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUMBER_OF_NODES),
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)


def open_tcp_port():
    """Open TCP port for incoming traffic.

    Print an exception if TCP port opening fails (e.g. if called again).
    """
    # Open TCP port for incoming traffic
    try:
        redshift = boto3.client('redshift',
                                region_name=REGION,
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET)
        ec2 = boto3.resource('ec2',
                             region_name=REGION,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET)
        cluster_properties = (
            redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)[
                'Clusters'][0]
        )
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        default_security_group = list(vpc.security_groups.all())[0]
        print(default_security_group)
        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)


def cluster_endpoint() -> str:
    """Get cluster endpoint.

    Example: returns dwhcluster.cyl4ucf0jibw.us-east-1.redshift.amazonaws.com.

    Returns
    -------
    str
        Cluster endpoint
    """
    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    cluster_properties = (
        redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)[
            'Clusters'][0]
    )
    return cluster_properties['Endpoint']['Address']


if __name__ == '__main__':
    role_arn = create_role()
    print('IAM role ARN: {}'.format(role_arn))
    attach_s3_read_only_access()
    create_cluster(role_arn)
    open_tcp_port()

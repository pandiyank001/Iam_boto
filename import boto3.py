import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import pandas as pd

def assume_role(account_id, role_name):
    print(f"Assuming role in account: {account_id}")
    sts_client = boto3.client('sts')
    try:
        response = sts_client.assume_role(
            RoleArn=f'arn:aws:iam::{account_id}:role/{role_name}',
            RoleSessionName='AssumeRoleSession'
        )
        print(f"Role assumed successfully for account: {account_id}")
        return response['Credentials']
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error assuming role in account {account_id}: {str(e)}")
        return None
    except ClientError as e:
        print(f"Client error: {e}")
        return None

def create_iam_client(credentials):
    print("Creating IAM client with assumed role credentials")
    return boto3.client(
        'iam',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

def list_users_with_active_keys(iam_client):
    print("Listing IAM users with active access keys")
    users_with_active_keys = []
    paginator = iam_client.get_paginator('list_users')
    for page in paginator.paginate():
        for user in page['Users']:
            response = iam_client.list_access_keys(UserName=user['UserName'])
            for key in response['AccessKeyMetadata']:
                if key['Status'] == 'Active':
                    users_with_active_keys.append({
                        'UserName': user['UserName'],
                        'AccessKeyId': key['AccessKeyId'],
                        'Status': key['Status'],
                        'CreateDate': key['CreateDate'].strftime('%Y-%m-%d %H:%M:%S')
                    })
    return users_with_active_keys

def aggregate_data(accounts, role_name):
    print("Aggregating data from multiple accounts")
    all_users = []
    for account_id in accounts:
        credentials = assume_role(account_id, role_name)
        if credentials:
            iam_client = create_iam_client(credentials)
            users_with_active_keys = list_users_with_active_keys(iam_client)
            for user in users_with_active_keys:
                user['AccountId'] = account_id
            all_users.extend(users_with_active_keys)
    return all_users

def generate_report(users_data, report_name='boto_credentials.csv'):
    print(f"Generating report: {report_name}")
    df = pd.DataFrame(users_data)
    df.to_csv(report_name, index=False)
    print(f"Report generated successfully: {report_name}")

def main(accounts, role_name, report_name='boto_credentials.csv'):
    print("Starting the IAM users report generation process")
    users_data = aggregate_data(accounts, role_name)
    generate_report(users_data, report_name)
    print("IAM users report generation process completed")

# Configuration
accounts = ['975049934351']
role_name = 'demobotorole'
report_name = 'boto_credentials.csv'

# Execute the main function
main(accounts, role_name, report_name)

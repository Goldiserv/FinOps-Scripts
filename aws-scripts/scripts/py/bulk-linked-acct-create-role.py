import csv
import json
import uuid
import boto3
from datetime import datetime

path = '/'
role_name = 'ToolName-IntegrationRole-ReadOnly'
description = 'Integration IAM Role for ToolName'
tags = [
    {
        'Key': 'createdBy',
        'Value': 'ToolName'
    }
]
policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
          "Sid": "ToolNameEC2Access",
          "Effect": "Allow",
          "Action": [
            "ec2:List*",
            "tag:GetResources",
            "tag:GetTagKeys",
            "tag:GetTagValues",
            "elasticloadbalancing:Describe*",
            "ec2:Describe*"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Sid": "ToolNameEC2AutoScalingAccess",
          "Effect": "Allow",
          "Action": [
            "autoscaling:Describe*"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Sid": "ToolNameOrganizationsAccess",
          "Effect": "Allow",
          "Action": [
            "organizations:List*",
            "organizations:Describe*"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Sid": "ToolNameCloudWatchAccess",
          "Effect": "Allow",
          "Action": [
            "cloudwatch:List*",
            "cloudwatch:Describe*",
            "cloudwatch:GetMetricStatistics"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Sid": "ToolNameSavingsPlansAccess",
          "Effect": "Allow",
          "Action": [
            "savingsplans:List*",
            "savingsplans:Describe*"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Sid": "ToolNameCostExplorerAccess",
          "Effect": "Allow",
          "Action": [
            "ce:List*",
            "ce:Describe*",
            "ce:Get*"
          ],
          "Resource": [
            "*"
          ]
        }
    ]
}


def role_arn_to_session(**args):
    """
    Usage :
        session = role_arn_to_session(
            RoleArn='arn:aws:iam::012345678901:role/example-role',
            RoleSessionName='ExampleSessionName')
        client = session.client('sqs')
    """
    try:
        client = boto3.client('sts')
        response = client.assume_role(**args)
        return boto3.Session(
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken']
        )
    except Exception as e:
        print("ERROR: Failed to assume role: ".format(e))
        raise Exception('ERROR: Failed to assume role')


def call_boto_pagination(func, key, token_name, *args, **kwargs):
    # call boto3 functions and if is pagination token continue make additional requests
    result = {}
    while kwargs.get(token_name, True):
        response = func(*args, **kwargs)
        result[key] = result.get(key, []) + response.get(key, [])
        kwargs[token_name] = response.pop(token_name, False)
    return result


def onboard_account(sts_client, sub_account, master_account_id, inner_role_name, remove):
    role_name = 'ToolName-IntegrationRole-ReadOnly'
    policy_name = 'ToolName-iam-role-sub-account-read-only'

    # skip master account
    if sub_account['Id'] == master_account_id:
        return

    # generate random external id
    sub_role_external_id = str(uuid.uuid4())

    # create trust policy
    trust_policy = {
        "Version": "2008-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::672188301118:root"
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": sub_role_external_id
                    }
                }
            }
        ]
    }

    try:
        # assume cross-account IAM role to connect to sub-account
        response = sts_client.assume_role(
            RoleArn="arn:aws:iam::{}:role/{}".format(sub_account['Id'], inner_role_name),
            RoleSessionName='InnerSession',
        )
        print(" - Successfully assumed cross-account IAM role")
    except Exception as e:
        print("ERROR Failed to assume cross-account IAM role ACCOUNT ID: {} || ERROR: {}".format(sub_account['Id'], e))
        return

    inner_session = boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken']
    )

    print(" - Connected to sub-account")

    # Create IAM client
    iam = inner_session.client('iam', region_name='eu-west-1')

    try:
        # create IAM role
        if not remove:
            try:
                response = iam.create_role(
                    Path=path,
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description=description,
                    MaxSessionDuration=3600,
                    Tags=tags
                )

                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print(" - ERROR  Failed to create IAM Role")
                    print(response)
            except Exception as e:
                print(" - Role already exists in account: {}, skipping...".format(sub_account['Id']))
                return {
                    "Account ID": sub_account['Id'],
                    "Account Alias": sub_account['Name'],
                    "ExternalID": "Account already onboarded",
                    "Role ARN": "Account already onboarded => Role ARN is not created"
                }

            sub_role_arn = response['Role']['Arn']
            sub_role_name = response['Role']['RoleName']

            print(" - IAM Role created: {}".format(sub_role_arn))
            print(" - ExternalID: {}".format(sub_role_external_id))

            try:
                response = iam.create_policy(
                    PolicyName=policy_name,
                    Path='/',
                    PolicyDocument=json.dumps(policy_document),
                    Description='ToolName Integration Role'
                )

                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print(" - ERROR  Failed to create IAM Policy")
                    print(response)

                sub_policy_arn = response['Policy']['Arn']
            except Exception as e:
                print(" - ERROR  Failed to create IAM Policy || Error: {}".format(e))
                return None

            print(" - IAM Policy created: {}".format(sub_policy_arn))

            try:
                iam.attach_role_policy(
                    PolicyArn=sub_policy_arn,
                    RoleName=sub_role_name
                )
            except Exception as e:
                print(" - ERROR  Failed to attach Policy to IAM Role || Error: {}".format(e))
                return None

            print(" - Done!")

            return {
                "Account ID": sub_account['Id'],
                "Account Alias": sub_account['Name'],
                "ExternalID": sub_role_external_id,
                "Role ARN": sub_role_arn
            }

        # if remove flag is True
        # remove ToolName Role and Policy
        else:
            try:
                list_roles = call_boto_pagination(
                    func=iam.list_roles,
                    key='Roles',
                    token_name="NextToken"
                ).get('Roles', [])

                attached_policies_dict = {}
                for role in list_roles:
                    try:
                        attached_policies = iam.list_attached_role_policies(
                            RoleName=role['RoleName'],
                            PathPrefix='/'
                        ).get('AttachedPolicies')
                        attached_policies_dict[role['RoleName']] = attached_policies
                    except Exception as e:
                        print(e)
            except Exception as e:
                print("ERROR Failed to list existing IAM Roles in account id: {}. Error: {}".format(
                    sub_account['Id'], e
                ))
                return

            for role_n in list_roles:
                for policy in attached_policies_dict[role_n['RoleName']]:
                    # find ToolName policy by name
                    if policy['PolicyName'] != policy_name:
                        continue

                    role_name = role_n['RoleName']

                    # detach IAM policy from the role
                    try:
                        response = iam.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                    except Exception as e:
                        print("ERROR Failed to detach role policy on account id: {}. Error: {}".format(
                            sub_account['Id'], e
                        ))

                    # delete policy
                    try:
                        response = iam.delete_policy(
                            PolicyArn=policy['PolicyArn']
                        )
                    except Exception as e:
                        print("ERROR Failed to delete IAM policy on account id: {}. Error: {}".format(
                            sub_account['Id'], e
                        ))

                    # delete integration IAM Role
                    try:
                        response = iam.delete_role(
                            RoleName=role_name
                        )

                        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                            print("ERROR Failed to delete IAM Role")
                            print(response)
                    except Exception as e:
                        print("ERROR Failed to delete IAM Role on account id: {}. Error: {}".format(
                            sub_account['Id'], e
                        ))

            print(" - Integration Role deleted on account: {}".format(sub_account['Id']))
            return

    except Exception as e:
        print(" - ERROR  Failed to delete IAM Role: Error: {}".format(e))
        return


def onboard_all_sub_accounts(inner_role_name, master_account_id, remove=False):
    # get organization details
    org_client = boto3.client('organizations', region_name="us-east-1")
    org_accounts = call_boto_pagination(
        func=org_client.list_accounts,
        key='Accounts',
        token_name="NextToken"
    ).get('Accounts', [])

    if len(org_accounts) == 0:
        print("ERROR Organization has no sub-accounts. exiting...")
        exit(1)

    print("INFO Found {} sub-accounts".format(len(org_accounts) - 1))
    print("")

    if remove:
        input_answer = input("Delete ToolName Integration IAM Role on all sub-accounts?      y/n: ")
    else:
        input_answer = input("Create ToolName Integration IAM Role on all sub-accounts?      y/n: ")

    if input_answer != "y":
        print("Aborting")
        exit()

    print("")

    # create STS client to assume cross-account IAM Role
    sts_client = boto3.client('sts', region_name="us-east-1")

    results = []
    for account in org_accounts:
        # skip inactive accounts
        if account['Status'] != 'ACTIVE':
            continue

        # skip master account
        if account['Id'] == master_account_id:
            continue

        print("INFO Working on account ID: {}".format(account['Id']))

        onboarded_acc_data = onboard_account(
            sts_client=sts_client,
            sub_account=account,
            master_account_id=master_account_id,
            inner_role_name=inner_role_name,
            remove=remove
        )

        if onboarded_acc_data is not None:
            results.append(onboarded_acc_data)
        else:
            results.append({
                "Account ID": account['Id'],
                "Account Alias": account['Name'],
                "ExternalID": "Failed to onboard",
                "Role ARN": "Failed to onboard => Role ARN is not created"
            })

        print(" ")

    # generate CSV file
    if not remove:
        csv_file = "ToolName-Onboarded-Accounts_{}.csv".format(datetime.today().strftime('%Y-%m-%d'))
        csv_columns = ['Account ID', 'Account Alias', 'ExternalID', 'Role ARN']
        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in results:
                    if data is not None:
                        writer.writerow(data)

            print("")
            print("Complete!")
            print("Result file: {}".format(csv_file))
        except IOError:
            print("ERROR Failed to export CSV file due to I/O error")
            exit(2)


if __name__ == "__main__":
    print("INFO Starting sub-account IAM Role creation")

    onboard_all_sub_accounts(
        # Organization Master Account
        master_account_id='-',  # <-------- please provide AWS Account ID

        # Cross-account IAM Role name
        inner_role_name='OrganizationAccountAccessRole',  # <-------- please provide cross-account role name

        # Set remove=True to delete all integration roles
        # remove=True
    )

import boto3
from botocore.exceptions import ClientError

# connecting to an aws service
# NOTE ---> developers using this code for the first time, you will need to add your credentials using the aws cli
# STEPS TO ADD YOUR AWS CREDENTIALS
# 1. If you are not already added to the aws service, contact the manager and get yourself added to the organisation
# 2. Make sure you are in the virtual environment, if not create one ---> `pip install virtualenv` ---> virtualenv env
# 3. Activate the env ---> env\Scripts\activate (for windows)
# 4. Install all the dependencies if not done yet ---> `pip install -r requirements.txt`
# 5. In the terminal of the repo ---> aws configure
# 6. Add your credentials and the code should work since you would be connected to the aws now!
dynamodb = boto3.resource(service_name='dynamodb')


def get_items_from_db(table_name):
    """
    Get all the items from 'table_name' table
    :param table_name: a string -> name of the table
    :return: a dictionary/JSON
    """
    try:
        # Connecting to the table
        table = dynamodb.Table(table_name)
        # Getting the documents
        response = table.scan()['Items']
        print(response)
        print("got something")
    except ClientError as e:
        print("Error")
    else:
        return response

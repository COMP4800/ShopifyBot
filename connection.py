import json
from datetime import date, timedelta
import boto3
from botocore.exceptions import ClientError
import requests
import pandas
import BulkOperationsQueries
from config import config

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
dynamodb_client = boto3.client(service_name='dynamodb')


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
        print(f"Error: {e}")
    else:
        return response


def get_bulk_data_url(store_name, start_date, end_date):
    """
    This function is just a demo of how the bulk query works in GraphQL
    :param start_date: The start date from which we want to get the data
    :param end_date: The end date until which we want the data
    :param store_name: the name of the store -> Considering there will be multiple stores
    :return: a string ->  the url containing the json data
    """
    store_url = f"https://{store_name}.myshopify.com"
    api_url = f'{store_url}/admin/api/2022-07/graphql.json'
    bulk_query = requests.post(api_url, auth=(config.APIkeys.KeepNatureSafeAPIKey,
                                              config.APIkeys.KeepNatureSafeAccessToken),
                               json={"query": BulkOperationsQueries.create_bulk_query(start_date, end_date)})
    print(bulk_query.json())
    while True:
        poll_query = requests.post(api_url, auth=(config.APIkeys.KeepNatureSafeAPIKey,
                                                  config.APIkeys.KeepNatureSafeAccessToken),
                                   json={"query": BulkOperationsQueries.PollQuery})
        print(poll_query.json())
        if poll_query.json()['data']['currentBulkOperation']['status'] == "COMPLETED":
            print(poll_query.json()['data']['currentBulkOperation']['url'])
            return poll_query.json()['data']['currentBulkOperation']['url']


def get_data(url):
    """
    This function returns the actual data parsed from the url we get from the Bulk Operation
    :param url: a String
    :return: a dictionary
    """
    res = requests.get(url)
    jsonObj = pandas.read_json(json.loads(json.dumps(res.text)), lines=True)
    json_file = json.loads(jsonObj.to_json(orient='table'))
    print(json_file["data"])
    print(url)
    data_to_be_pushed = []
    for each_line in json_file["data"]:
        line = {}
        if each_line['id'] is None:
            line["OrderID"] = ""
        else:
            line["OrderID"] = each_line['id']

        if each_line['name'] is None:
            line["OrderName"] = ""
        else:
            line["OrderName"] = each_line['name']

        if each_line['customer'] is None:
            line["CustomerID"] = ""
            line["TotalOrdersMadeByTheCustomer"] = ""
            line["AverageOrderValue"] = ""
            line["FirstOrderDate"] = ""
            line["IsFirstOrder"] = False
        else:
            line["CustomerID"] = each_line['customer']['id']
            line["TotalOrdersMadeByTheCustomer"] = each_line['customer']['numberOfOrders']
            line["AverageOrderValue"] = each_line['customer']['averageOrderAmountV2']['amount']
            line["FirstOrderDate"] = each_line['customer']['createdAt']
            if each_line['customer']['numberOfOrders'] == '1':
                line["IsFirstOrder"] = True
            else:
                line["IsFirstOrder"] = False

        if each_line['createdAt'] is None:
            line["OrderDate"] = ""
        else:
            line["OrderDate"] = each_line['createdAt']

        if each_line['subtotalPriceSet'] is None:
            line["GrossSales"] = "0.00"
        else:
            line["GrossSales"] = each_line['subtotalPriceSet']['shopMoney']['amount']

        if each_line["cartDiscountAmountSet"] is None:
            line["Discounts"] = "0.00"
        else:
            line["Discounts"] = each_line["cartDiscountAmountSet"]['shopMoney']['amount']

        if each_line["totalRefundedSet"] is None:
            line["Returns"] = "0.00"
        else:
            line["Returns"] = each_line["totalRefundedSet"]['shopMoney']['amount']

        if each_line["currentSubtotalPriceSet"] is None:
            line["NetSales"] = "0.00"
        else:
            line["NetSales"] = each_line["currentSubtotalPriceSet"]['shopMoney']['amount']

        if each_line["totalShippingPriceSet"] is None:
            line["Shipping"] = "0.00"
        else:
            line["Shipping"] = each_line["totalShippingPriceSet"]['shopMoney']['amount']

        if each_line["totalTaxSet"] is None:
            line["Taxes"] = "0.00"
        else:
            line["Taxes"] = each_line["totalTaxSet"]['shopMoney']['amount']

        if each_line["totalPriceSet"] is None:
            line["TotalSales"] = "0.00"
        else:
            line["TotalSales"] = each_line["totalPriceSet"]['shopMoney']['amount']

        print(line)
        data_to_be_pushed.append(line)
    return data_to_be_pushed


def get_shops_creation_date(shop_name):
    res = requests.get(
        f"https://{config.APIkeys.KeepNatureSafeAPIKey}:{config.APIkeys.KeepNatureSafeAccessToken}@{shop_name}"
        f".myshopify.com/admin/api/2022-10/shop.json")
    return json.loads(json.dumps(res.json()))['shop']['created_at']


def create_and_write_to_aws(table_name, data):
    """
    Create a tabel first and write the data
    :param table_name: a string
    :param data: a python dictionary -> JSON-like
    """
    dynamodb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'OrderID',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'OrderDate',
                'AttributeType': 'S'

            }
        ],
        KeySchema=[
            {
                'AttributeName': 'OrderID',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'OrderDate',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },
        TableName=table_name
    )
    print(f'{table_name} Table created, Now Writing all the past orders')

    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(TableName=table_name)
    dynamodb_table = dynamodb.Table(table_name)

    try:
        with dynamodb_table.batch_writer() as writer:
            for item in data:
                writer.put_item(Item=item)
            print("Success")
    except ClientError as e:
        print(f"Couldn't load data to table {table_name} - {e}")


def write_to_aws(table_name, data):
    """
    Write data to db
    :param table_name: a string
    :param data: a python dictionary -> JSON-like
    """
    dynamodb_table = dynamodb.Table(table_name)
    try:
        with dynamodb_table.batch_writer() as writer:
            for item in data:
                writer.put_item(Item=item)
        print("Success")
    except ClientError:
        print(f"Couldn't load data to table {table_name}")


def wrapper(client_name):
    """
    Runs the Code for one client
    :param client_name: a string
    """
    existing_tables = dynamodb_client.list_tables()['TableNames']
    last_day_of_previous_month = date.today().replace(day=1) - timedelta(days=1)
    last_day_of_previous_month_string = str(last_day_of_previous_month)
    first_day_of_previous_month_string = str(date.today().replace(day=1) -
                                             timedelta(days=last_day_of_previous_month.day))

    if client_name not in existing_tables:
        shops_creation_date = get_shops_creation_date(client_name)
        bulk_data_url = get_bulk_data_url(client_name, shops_creation_date, last_day_of_previous_month)
        data = get_data(bulk_data_url)
        create_and_write_to_aws(client_name, data)
        print(bulk_data_url)
    else:
        bulk_data_url = get_bulk_data_url("keep-it-wild-az", first_day_of_previous_month_string,
                                          f"{last_day_of_previous_month_string}T24:00:00")
        # bulk_data_url = get_bulk_data_url("keep-it-wild-az", "2022-01-01T00:00:00",
        #                                   f"{last_day_of_previous_month_string}T24:00:00")
        data = get_data(bulk_data_url)
        write_to_aws(client_name, data)
        print(bulk_data_url)

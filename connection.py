import datetime
import json
from datetime import date, timedelta
import boto3
from botocore.exceptions import ClientError
import requests
import pandas
import BulkOperationsQueries
from config import config
from collections import defaultdict

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


def get_bulk_data_url(store_name: str, start_date: str, end_date: str):
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


def get_data(url: str):
    """
    This function returns the actual data parsed from the url we get from the Bulk Operation
    :param url: a String
    :return: a dictionary
    """
    res = requests.get(url)
    jsonObj = pandas.read_json(json.loads(json.dumps(res.text)), lines=True)
    json_file = json.loads(jsonObj.to_json(orient='table'))
    # print(json_file["data"])
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
            # line["IsFirstOrder"] = False
        else:
            line["CustomerID"] = each_line['customer']['id']
            line["TotalOrdersMadeByTheCustomer"] = each_line['customer']['numberOfOrders']
            line["AverageOrderValue"] = each_line['customer']['averageOrderAmountV2']['amount']
            line["FirstOrderDate"] = each_line['customer']['createdAt']

        if each_line['createdAt'] is None:
            line["OrderDate"] = ""
        else:
            date_for_each_order = str(each_line['createdAt'])[:-1]
            line["OrderDate"] = (datetime.datetime.fromisoformat(date_for_each_order) - timedelta(hours=8)).isoformat()
            line["Year"] = f'{datetime.datetime.fromisoformat(line["OrderDate"]).year}'
            # f'-{datetime.datetime.fromisoformat(line["OrderDate"]).month}'
        # if each_line['customer']['numberOfOrders'] == '1':
        #     line["IsFirstOrder"] = True
        # else:
        #     line["IsFirstOrder"] = False
        if each_line["customer"] is not None:
            order_date_month_year = f'{datetime.datetime.fromisoformat(str(each_line["createdAt"])[:-1]).year}-{datetime.datetime.fromisoformat(str(each_line["createdAt"])[:-1]).month}'
            first_order_month_year = f'{datetime.datetime.fromisoformat(str(each_line["customer"]["createdAt"])[:-1]).year}-{datetime.datetime.fromisoformat(str(each_line["customer"]["createdAt"])[:-1]).month} '
            if order_date_month_year == first_order_month_year:
                line["IsFirstOrderMonth"] = True
            else:
                line["IsFirstOrderMonth"] = False
        else:
            line["IsFirstOrderMonth"] = None

        if each_line["currentTotalDiscountsSet"] is None:
            line["Discounts"] = "0.00"
        else:
            line["Discounts"] = each_line["currentTotalDiscountsSet"]['shopMoney']['amount']

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
            if each_line["totalRefundedShippingSet"] is not None:
                line["Shipping"] = str(float(each_line["totalShippingPriceSet"]['shopMoney']['amount']) -
                                       float(each_line["totalRefundedShippingSet"]['shopMoney']['amount']))

        if each_line["totalTaxSet"] is None:
            line["Taxes"] = "0.00"
        else:
            line["Taxes"] = each_line["totalTaxSet"]['shopMoney']['amount']

        if each_line["currentTotalPriceSet"] is None:
            line["TotalSales"] = "0.00"
        else:
            line["TotalSales"] = each_line["currentTotalPriceSet"]['shopMoney']['amount']

        if each_line['currentSubtotalPriceSet'] is None:
            line["GrossSales"] = "0.00"
        else:
            line["GrossSales"] = str(float(line["NetSales"]) +
                                     float(line["Discounts"]) +
                                     float(line["Returns"]))

        # print(line)
        data_to_be_pushed.append(line)
    return data_to_be_pushed


def get_shops_creation_date(shop_name: str):
    res = requests.get(
        f"https://{config.APIkeys.KeepNatureSafeAPIKey}:{config.APIkeys.KeepNatureSafeAccessToken}@{shop_name}"
        f".myshopify.com/admin/api/2022-10/shop.json")
    return json.loads(json.dumps(res.json()))['shop']['created_at']


def create_and_write_to_aws_with_lsi(table_name: str, data: list):
    """
    Create a tabel first and write the data with using Dynamo Db's LSI -> this function is to be used for the first time
    pulls only.
    :param table_name: a string
    :param data: a python dictionary -> JSON-like
    """
    table_name = f'{table_name}-raw'
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'Year',
                'AttributeType': 'S'
            },
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
                'AttributeName': 'Year',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'OrderID',
                'KeyType': 'RANGE'
            }
        ],
        LocalSecondaryIndexes=[
            {
                'IndexName': 'OrdersByMonthAndDate',
                'KeySchema': [
                    {
                        'AttributeName': 'Year',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'OrderDate',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },

    )
    print(f'{table_name}-raw Table created, Now Writing all the past orders')

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


def create_and_write_to_aws_with_lsi_transformed(table_name: str, data: list):
    """
    Create a tabel first and write the data with an LSI -> transformations -> -> this function is to be used for the
    first time pulls only.
    :param table_name: a string
    :param data: a python dictionary -> JSON-like
    """
    table_name = f'{table_name}-transformed'
    dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'Date',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Type',
                'AttributeType': 'S'

            }
        ],
        KeySchema=[
            {
                'AttributeName': 'Date',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Type',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },

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


def write_to_aws(table_name: str, data: list):
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


def wrapper(client_name: str):
    """
    Runs the Code for one client
    :param client_name: a string
    """
    existing_tables = dynamodb_client.list_tables()['TableNames']
    last_day_of_previous_month = date.today().replace(day=1) - timedelta(days=1)
    first_day_of_this_month = f'{datetime.datetime.today().replace(day=1)}'
    # last_day_of_previous_month_string = f'{last_day_of_previous_month.year}-{last_day_of_previous_month.month}' \
    #                                     f'-{last_day_of_previous_month.day + 1}'
    first_day_of_previous_month_string = str(date.today().replace(day=1) -
                                             timedelta(days=last_day_of_previous_month.day))
    shops_creation_date = get_shops_creation_date(client_name)

    if f'{client_name}-raw' and f'{client_name}-transformed' not in existing_tables:

        # GETTING THE RAW DATA

        bulk_data_url = get_bulk_data_url(client_name, shops_creation_date, first_day_of_this_month)
        data = get_data(bulk_data_url)

        # WRITING THE RAW DATA FOR THE FIRST TIME

        create_and_write_to_aws_with_lsi(client_name, data)

        # CONVERTING RAW DATA TO TRANSFORMATIONS

        split_data = split_data_by_year_and_month(data)
        transformed_data = transform_split_data(split_data)

        # WRITING THE TRANSFORMATIONS FOR THE FIRST TIME

        create_and_write_to_aws_with_lsi_transformed(client_name, transformed_data)
        print(bulk_data_url)
    else:

        # GETTING THE RAW DATA

        bulk_data_url = get_bulk_data_url(client_name, first_day_of_previous_month_string,
                                          first_day_of_this_month)
        data = get_data(bulk_data_url)

        # WRITING THE RAW DATA MONTHLY

        write_to_aws(f'{client_name}-raw', data)

        # CONVERTING RAW DATA TO TRANSFORMATIONS

        split_data = split_data_by_year_and_month(data)
        transformed_data = transform_split_data(split_data)

        # WRITING THE TRANSFORMATIONS MONTHLY

        write_to_aws(f'{client_name}-transformed', transformed_data)
    print(bulk_data_url)


def get_data_from_shopify(client_name: str, start_date: str, end_date: str):
    # NOTE - THIS IS A HELPER FUNCTION IN CASE THE DEVELOPERS WANT A XLSX SHEET OF RAW DATA FROM THE CODE
    """
    This method can be used to test data by getting an Excel file.
    :param client_name: name of the shop
    :param start_date: a string (yyyy-mm-dd)
    :param end_date: a string (yyyy-mm-dd) the day should be one more than the last date you actually want
    """
    url = get_bulk_data_url(client_name, start_date, end_date)
    data = get_data(url)
    df = pandas.DataFrame(data=data)
    df.to_excel("Shopify_Data_From_GraphQl_API-123.xlsx", index=False)
    print(len(data))
    print(url)


def split_data_by_year_and_month(data: list):
    """
    This function separates the collected data into chunks of monthly data.
    :param data: a list of orders
    :return: a list of dictionaries where each key is a month-year combination and value is a list of all the orders in
    that month-year.
    """
    data_by_month = defaultdict(list)
    for each_line in data:
        date_for_each_order = each_line["OrderDate"]
        year = datetime.datetime.fromisoformat(date_for_each_order).year
        month = datetime.datetime.fromisoformat(date_for_each_order).month
        data_by_month[f'{year}-{month}'].append(each_line)
    data_separated_by_month_and_year = []
    for i in data_by_month.items():
        each_months_data = {f'{i[0]}': i[1]}
        data_separated_by_month_and_year.append(each_months_data)
    # print(len(data_separated_by_month_and_year))
    return data_separated_by_month_and_year


def transform_split_data(data: list):
    """
    This function is responsible for transforming the data into groups of first-time orders and multiple orders and then
    add Total Sales, AOV and Average orders.
    :param data: a list of python dictionaries where the keys are in the format "yyyy-mm"
    :return: a list of python dictionaries(the transformed data)
    """
    transformed_data = []
    for each_month_year in data:
        # This is the list of the dictionaries for a single yyyy-mm month.
        each_month_year_values = list(each_month_year.values())

        # This is the yyyy-mmm year-month for the list of dictionaries mentioned above.
        each_month_year_key = str(list(each_month_year.keys())[0])
        list_of_monthly_orders = each_month_year_values[0]

        first_order_set = []
        multiple_orders_set = []
        # This is the count of order values for first timers and multiple timers.
        first_time_count = 0
        multiple_count = 0

        # This is the sales numbers for first timers and multiple timers.
        first_time_sales = 0
        multiple_sales = 0

        # This is the data structure that would hold the transformations for each month.
        first_time_transformed = {"Type": "First", "Date": f'{each_month_year_key}'}
        multiple_transformed = {"Type": "Multiple", "Date": f'{each_month_year_key}'}

        for orders in list_of_monthly_orders:
            if orders["IsFirstOrderMonth"] is not None:
                if orders["IsFirstOrderMonth"] is True:
                    first_time_count += 1
                    first_time_sales += float(orders["TotalSales"])
                    first_order_set.append(orders["CustomerID"])
                elif orders["IsFirstOrderMonth"] is False:
                    multiple_count += 1
                    multiple_sales += float(orders["TotalSales"])
                    multiple_orders_set.append(orders["CustomerID"])

        # Setting the Count for a particular year-month
        first_time_transformed["Count"] = str(first_time_count)
        multiple_transformed["Count"] = str(multiple_count)

        # Setting the Total Sales for a particular year-month
        first_time_transformed["TotalSales"] = f'{(round(first_time_sales, 2))}'
        multiple_transformed["TotalSales"] = f'{round(multiple_sales, 2)}'

        # Setting the AOV for a particular year-month
        first_time_transformed["AOV"] = f'{round(float(first_time_sales / first_time_count), 2)}'
        multiple_transformed["AOV"] = f'{round(float(multiple_sales / multiple_count), 2)}'

        # Setting the Average Orders for a particular year-month.
        first_time_transformed["Avg. Orders"] = f'{round(first_time_count / len(set(first_order_set)), 3)}'
        multiple_transformed["Avg. Orders"] = f'{round(multiple_count / len(set(multiple_orders_set)), 3)}'

        # Adding the first time and Multiple time transformations for a particular month to the all-time
        # transformations list
        transformed_data.append(first_time_transformed)
        transformed_data.append(multiple_transformed)
    for items in transformed_data:
        print(items)
    return transformed_data

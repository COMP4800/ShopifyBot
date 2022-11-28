import datetime
import json
from datetime import date, timedelta
import boto3
from botocore.exceptions import ClientError
import requests
import pandas
import BulkOperationsQueries
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


def add_is_first_order(url: str) -> list:
    res = requests.get(url)
    jsonObj = pandas.read_json(json.loads(json.dumps(res.text)), lines=True)
    json_file = json.loads(jsonObj.to_json(orient='table'))

    resp = json_file['data']
    list_of_orders = [resp[0]]
    list_of_each_customers_orders = []
    resp.pop(0)

    for each_line in resp:
        if each_line['id'] is not None:
            date_for_each_order = str(list_of_orders[len(list_of_orders) - 1]['createdAt'])[:-1]
            date_for_each_order = (
                        datetime.datetime.fromisoformat(date_for_each_order) - datetime.timedelta(hours=8)).isoformat()
            if len(list_of_each_customers_orders) > 0:
                if min(list_of_each_customers_orders) == date_for_each_order:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrder'] = "True"
                else:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrder'] = "False"
                list_of_orders[len(list_of_orders) - 1]['FirstOrderDate'] = min(list_of_each_customers_orders)
            list_of_orders.append(each_line)
            list_of_each_customers_orders = []
        else:
            order = str(each_line['createdAt'])[:-1]
            order = (datetime.datetime.fromisoformat(order) - datetime.timedelta(hours=8)).isoformat()
            list_of_each_customers_orders.append(order)

    last_order_date = str(list_of_orders[len(list_of_orders) - 1]['createdAt'])[:-1]
    last_order_date = (datetime.datetime.fromisoformat(last_order_date) - datetime.timedelta(hours=8)).isoformat()
    if min(list_of_each_customers_orders) == last_order_date:
        list_of_orders[len(list_of_orders) - 1]['IsFirstOrder'] = "True"
    else:
        list_of_orders[len(list_of_orders) - 1]['IsFirstOrder'] = "False"
    list_of_orders[len(list_of_orders) - 1]['FirstOrderDate'] = min(list_of_each_customers_orders)

    return list_of_orders


def get_data(url: str) -> list:
    """
    This function returns the actual data parsed from the url we get from the Bulk Operation
    :param url: a String
    :return: a dictionary
    """
    data = add_is_first_order(url)
    data_to_be_pushed = []
    for each_line in data:
        line = {}
        if each_line['id'] is None:
            line["OrderID"] = ""
        else:
            line["OrderID"] = each_line['id']

        if each_line['name'] is None:
            line["OrderName"] = ""
        else:
            line["OrderName"] = each_line['name']

        if each_line['createdAt'] is None:
            line["OrderDate"] = ""
        else:
            date_for_each_order = str(each_line['createdAt'])[:-1]
            line["OrderDate"] = (
                    datetime.datetime.fromisoformat(date_for_each_order) - datetime.timedelta(hours=8)).isoformat()
            line["Year"] = f'{datetime.datetime.fromisoformat(line["OrderDate"]).year}'

        if each_line['customer'] is None:
            line["CustomerID"] = ""
            line["TotalOrdersMadeByTheCustomer"] = ""
            line["AverageOrderValue"] = ""
        else:
            line["CustomerID"] = each_line['customer']['id']
            line["TotalOrdersMadeByTheCustomer"] = each_line['customer']['numberOfOrders']
            line["AverageOrderValue"] = each_line['customer']['averageOrderAmountV2']['amount']

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

        if 'FirstOrderDate' not in each_line.keys():
            line['FirstOrderDate'] = each_line['createdAt']
            line['IsFirstOrder'] = True
        else:
            line['FirstOrderDate'] = each_line['FirstOrderDate']
            line['IsFirstOrder'] = each_line['IsFirstOrder']

        data_to_be_pushed.append(line)

        # print(line)
    return data_to_be_pushed


def get_bulk_data_url(store_name: str, start_date: str, end_date: str, api_key: str, access_token: str) -> str:
    """
    This function is just a demo of how the bulk query works in GraphQL
    :param access_token: a string -> the access token of that client
    :param api_key: a string -> the api key of that client
    :param start_date: The start date from which we want to get the data
    :param end_date: The end date until which we want the data
    :param store_name: the name of the store -> Considering there will be multiple stores
    :return: a string ->  the url containing the json data
    """
    store_url = f"https://{store_name}.myshopify.com"
    api_url = f'{store_url}/admin/api/2022-07/graphql.json'

    bulk_query = requests.post(api_url, auth=(api_key, access_token),
                               json={"query": BulkOperationsQueries.create_bulk_query(start_date, end_date)})
    print(bulk_query.json())
    while True:
        poll_query = requests.post(api_url, auth=(api_key, access_token),
                                   json={"query": BulkOperationsQueries.PollQuery})
        print(poll_query.json())
        if poll_query.json()['data']['currentBulkOperation']['status'] == "COMPLETED":
            # url = poll_query.json()['data']['currentBulkOperation']['url']
            # data = get_data(url)
            # return data
            print(poll_query.json()['data']['currentBulkOperation']['url'])
            return poll_query.json()['data']['currentBulkOperation']['url']


def get_shops_creation_date(shop_name: str, api_key: str, access_token: str) -> str:
    res = requests.get(
        f"https://{api_key}:{access_token}@{shop_name}"
        f".myshopify.com/admin/api/2022-10/shop.json")
    return json.loads(json.dumps(res.json()))['shop']['created_at']


def create_and_write_to_aws_with_lsi(table_name: str, data: list) -> None:
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


def create_and_write_to_aws_with_lsi_transformed(table_name: str, data: list) -> None:
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


def write_to_aws(table_name: str, data: list) -> None:
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


def wrapper() -> None:
    """
    Runs the Code for one client
    """
    existing_tables = dynamodb_client.list_tables()['TableNames']
    last_day_of_previous_month = date.today().replace(day=1) - timedelta(days=1)
    first_day_of_this_month = str(datetime.datetime.today().replace(day=1).isoformat())
    first_day_of_previous_month_string = str(date.today().replace(day=1) -
                                             timedelta(days=last_day_of_previous_month.day))
    all_clients = get_all_clients()
    for client in all_clients:
        client_name = client["name"]
        client_api_key = client["API-Key"]
        client_access_token = client["Access-Token"]
        shops_creation_date = get_shops_creation_date(client_name, client_api_key, client_access_token)
        if f'{client_name}-raw' and f'{client_name}-transformed' not in existing_tables:
            # GETTING THE RAW DATA

            bulk_data_url = get_bulk_data_url(client_name, shops_creation_date, first_day_of_this_month,
                                              client_api_key, client_access_token)
            data = get_data(bulk_data_url)
            # "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/9fjmigmqno241a6l66k45zzmbyas?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1670160211&Signature=LdoNMun7fGHz7Hl0AJd5%2BXC53p%2BjdbCM%2B%2F5WhgPHQpQuLTRYNVMPwRUriNjJeJMFw1vs61FUmG%2FsGDOY0yGUuIV2TkjbTxb9VOFQnY5ASa4vj%2FZlt34Zd4IY%2F6LjimZO3quSI2320tBOygyRHw0xW1UdOWwuE8cXz8J2gWhovJ%2FY9iCCnHWmmmAZ6UCTNthOmsgrgiZ1j7n0Xv3Mk1I58QkCG7AyixKlYSYWovqiaw4ovT%2BBlaIh8gUKYKAm%2FPbQh4%2FsUs5aVImOEf5lEfcg1lTFJRTNA8Tw8KAHBGXhV1Boohv1Zowzo%2BLbeC6JMgB4F7RkB%2BJFhPR2lfzNkJEiIA%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-2182487015615.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-2182487015615.jsonl&response-content-type=application%2Fjsonl"

            # print(bulk_data_url)

            # WRITING THE RAW DATA FOR THE FIRST TIME

            create_and_write_to_aws_with_lsi(client_name, data)

            # CONVERTING RAW DATA TO TRANSFORMATIONS

            split_data = split_data_by_year_and_month(data)
            transformed_data = transform_split_data(split_data)

            # WRITING THE TRANSFORMATIONS FOR THE FIRST TIME

            create_and_write_to_aws_with_lsi_transformed(client_name, transformed_data)
        else:
            # Checking if this is the first day of the month so that monthly data from last month can be pulled......
            if datetime.datetime.today().day == 1:

                # GETTING THE RAW DATA

                bulk_data_url = get_bulk_data_url(client_name, first_day_of_previous_month_string,
                                                  first_day_of_this_month, client_api_key, client_access_token)
                data = get_data(bulk_data_url)

                # WRITING THE RAW DATA MONTHLY

                write_to_aws(f'{client_name}-raw', data)

                # CONVERTING RAW DATA TO TRANSFORMATIONS

                split_data = split_data_by_year_and_month(data)
                transformed_data = transform_split_data(split_data)

                # WRITING THE TRANSFORMATIONS MONTHLY

                write_to_aws(f'{client_name}-transformed', transformed_data)
        # print(bulk_data_url)


def split_data_by_year_and_month(data: list) -> list:
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


def transform_split_data(data: list) -> list:
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
            # if orders["IsFirstOrderMonth"] == "True":
            if orders["IsFirstOrder"] == "True":
                first_time_count += 1
                first_time_sales += float(orders["TotalSales"])
                first_order_set.append(orders["CustomerID"])
            # elif orders["IsFirstOrderMonth"] == "False":
            elif orders["IsFirstOrder"] == "False":
                multiple_count += 1
                multiple_sales += float(orders["TotalSales"])
                multiple_orders_set.append(orders["CustomerID"])

        # Setting the Count for a particular year-month

        first_time_transformed["Count"] = str(first_time_count)
        multiple_transformed["Count"] = str(multiple_count)

        # Setting the Total Sales for a particular year-month
        first_time_transformed["TotalSales"] = f'{(round(first_time_sales, 2))}'
        multiple_transformed["TotalSales"] = f'{round(multiple_sales, 2)}'

        if first_time_count == 0 or first_time_sales == 0:
            first_time_transformed["AOV"] = "0"
            first_time_transformed["Avg. Orders"] = "0"
        elif first_time_count != 0 or first_time_sales != 0:
            first_time_transformed["AOV"] = f'{round(float(first_time_sales / first_time_count), 2)}'
            first_time_transformed["Avg. Orders"] = f'{round(first_time_count / len(set(first_order_set)), 3)}'
        if multiple_count == 0 or multiple_sales == 0:
            multiple_transformed["AOV"] = "0"
            multiple_transformed["Avg. Orders"] = "0"
        elif multiple_count != 0 or multiple_sales != 0:
            multiple_transformed["AOV"] = f'{round(float(multiple_sales / multiple_count), 2)}'
            multiple_transformed["Avg. Orders"] = f'{round(multiple_count / len(set(multiple_orders_set)), 3)}'

        # transformations list
        transformed_data.append(first_time_transformed)
        transformed_data.append(multiple_transformed)
    for items in transformed_data:
        print(items)
    return transformed_data


def get_all_clients() -> list[dict]:
    """
    Get a list of all the clients added through the Google sheets.
    :return: a list of dictionaries
    """
    table = dynamodb.Table("ClientInfo")
    response = table.scan()
    return response["Items"]


def get_api_keys_and_access_keys_from_shopify(client_name: str) -> dict:
    """
    This is a helper function if the developers want to get api key of a single client
    :param client_name: the name of the client
    :return: a dictionary
    """
    table = dynamodb.Table("ClientInfo")
    response = table.scan()
    data = response["Items"]
    for client in data:
        if client['name'] == client_name:
            return client
    return {"message": "client does not exist"}


def stop_query(client_name: str, bulk_operation_id: str) -> requests.Response:
    """
    This function is used to cancel an ongoing bulk query.
    :param client_name: the name of the shopify store
    :param bulk_operation_id: the id of the bulk operation that needs to be canceled.
    :return: a Response object
    """
    store_url = f"https://{client_name}.myshopify.com"
    api_url = f'{store_url}/admin/api/2022-07/graphql.json'
    client = get_api_keys_and_access_keys_from_shopify(client_name)
    api_key = client["API-Key"]
    access_token = client["Access-Token"]
    bulk_query = requests.post(api_url,
                               auth=(api_key, access_token),
                               json={"query": BulkOperationsQueries.get_cancel_query(bulk_operation_id)})
    return bulk_query


def add_is_first_order_month(url: str) -> list:
    # Getting raw data from Shopify
    res = requests.get(url)
    jsonObj = pandas.read_json(json.loads(json.dumps(res.text)), lines=True)
    json_file = json.loads(jsonObj.to_json(orient='table'))

    # Declaring helper lists to identify first-time user vs returning users
    list_of_orders = []
    list_of_each_customers_orders = []

    # Adding IsFirstOrderMonth and FirstOrderDate to the raw data-set
    for each_line in json_file['data']:
        if each_line['id'] is not None:
            date_for_each_order = str(each_line['createdAt'])[:-1]
            date_for_each_order = (
                        datetime.datetime.fromisoformat(date_for_each_order) - datetime.timedelta(hours=8)).isoformat()
            order_month = datetime.datetime.fromisoformat(date_for_each_order).month
            order_year = datetime.datetime.fromisoformat(date_for_each_order).year
            flag = 0
            single_order_flag = 0
            if len(list_of_each_customers_orders) == 1:
                each_cust_order_month_single = datetime.datetime.fromisoformat(list_of_each_customers_orders[0]).month
                each_cust_order_year_single = datetime.datetime.fromisoformat(list_of_each_customers_orders[0]).year
                if each_cust_order_month_single != order_month or each_cust_order_year_single != order_year:
                    single_order_flag = 1
                if single_order_flag == 1:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = False
                else:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = True
                list_of_orders[len(list_of_orders) - 1]['FirstOrderDate'] = min(list_of_each_customers_orders)
            if len(list_of_each_customers_orders) > 1:
                for each_cust_order in list_of_each_customers_orders:
                    each_cust_order_month = datetime.datetime.fromisoformat(each_cust_order).month
                    each_cust_order_year = datetime.datetime.fromisoformat(each_cust_order).year
                    if each_cust_order_year != order_year or each_cust_order_month != order_month:
                        flag = 1
                if flag == 1:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = False
                else:
                    list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = True
                list_of_orders[len(list_of_orders) - 1]['FirstOrderDate'] = min(list_of_each_customers_orders)

            list_of_orders.append(each_line)

            # print(list_of_each_customers_orders)

            list_of_each_customers_orders = []
        else:
            order = str(each_line['createdAt'])[:-1]
            order = (datetime.datetime.fromisoformat(order) - datetime.timedelta(hours=8)).isoformat()
            list_of_each_customers_orders.append(order)

    # Declaring helper variables for the last order since the loop only processes till the second-last item
    last_order_date = str(list_of_orders[len(list_of_orders) - 1]['createdAt'])[:-1]
    last_order_date = (datetime.datetime.fromisoformat(last_order_date) - datetime.timedelta(hours=8)).isoformat()
    last_order_month = datetime.datetime.fromisoformat(last_order_date).month
    last_order_year = datetime.datetime.fromisoformat(last_order_date).year
    last_flag = 0

    # Processing the last remaining order
    for each_cust_order in list_of_each_customers_orders:
        each_cust_order_month = datetime.datetime.fromisoformat(each_cust_order).month
        each_cust_order_year = datetime.datetime.fromisoformat(each_cust_order).year
        if each_cust_order_year != last_order_year or each_cust_order_month != last_order_month:
            last_flag = 1
    if last_flag == 1:
        list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = False
    else:
        list_of_orders[len(list_of_orders) - 1]['IsFirstOrderMonth'] = True

    list_of_orders[len(list_of_orders) - 1]['FirstOrderDate'] = min(list_of_each_customers_orders)

    return list_of_orders

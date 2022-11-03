from connection import wrapper, get_data_from_shopify

# This is the initialization of this repo

list_of_clients = [
    "keep-it-wild-az",
]


def main():
    for client in list_of_clients:
        wrapper(client)
        # Anything below this line will be executed for all the clients in the list_of_clients

        # Get the date when the shop was created
        # shops_creation_date = get_shops_creation_date(client)

        # Getting the Url which contains non formatted raw data from shopify
        # bulk_data_url = get_bulk_data_url(list_of_clients[0], "2022-09-1", "2022-09-30")

        # Creating a formatted raw data file from the url we get in the previous step
        # formatted_data_to_be_pushed = get_data(bulk_data_url)

        # This will write the formatted raw data to AWS to the dedicated client table
        # write_raw_data_to_db(client, formatted_data_to_be_pushed)


if __name__ == '__main__':
    main()

from connection import wrapper, get_data_from_shopify, split_data_by_year_and_month, transform_split_data, \
    get_bulk_data_url, get_data, create_and_write_to_aws_with_lsi, get_shops_creation_date, create_and_write_to_aws_with_lsi_transformed

# This is the initialization of this repo

list_of_clients = [
    "keep-it-wild-az",
]


def main():
    for client in list_of_clients:

        # creation_date = get_shops_creation_date("keep-it-wild-az")
        # url = get_bulk_data_url(client, creation_date, "2022-11-01")
        # data = get_data(url)
        # segregated_data = split_data_by_year_and_month(data)
        # transform_split_data(segregated_data)

        # Trying LSI
        # url = get_bulk_data_url(client, "2022-05-01", "2022-11-01")
        # data = get_data("https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/14wnz0n7sku5etfnybk03h5rn7wx?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1668144543&Signature=MGDIO1PN%2Fs4p%2BooMgmej6Lk5145EKPi0ilQBstdmtsqHqLpdYsSqyF%2BgQdRY4pz9xdRI6Fh%2B1JJqw%2Br2xD9JfXvlrI%2F7c6MynJRxrYfkZ2D4kP8fCOe8TE58EgccX9vPvnaLd%2FKxF16po3N91oF0QRLSwq5Eg6EmgVMRGyU8qDIpNlUM07bTUp9OMGwgcoSCX9TwxGHkWxNYlmbnoXLFnHBf8zHwxGKtB%2Fw6qX3mgfES1kBOd5JgNZZrWrKwoMfVZ3YRB7SsEpaIId0gmszEy5vyek5DnhlY420ZM2dVs56wT5Bae6QwNYVx7XVoqY6l3DR%2FPHEliTUBBgb2EganlQ%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-2068352106687.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-2068352106687.jsonl&response-content-type=application%2Fjsonl")
        # create_and_write_to_aws_with_lsi(f'{client}-raw-with-LSI-only-Year-PK', data)

        # data = []
        data = split_data_by_year_and_month()
        transformed_data = transform_split_data(data)
        # create_and_write_to_aws_with_lsi_transformed(client, transformed_data)
        # wrapper(client)
        # get_data_from_shopify(client, "2022-01-01", "2022-11-01")

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

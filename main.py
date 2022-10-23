from connection import get_bulk_data

# This is the initialization of this repo

list_of_clients = [
    'EightXDeveloperTestTable'
]


def main():
    print(get_bulk_data("keep-it-wild-az"))
    # for client in list_of_clients:
    # print(get_bulk_data("keep-it-wild-az"))
    # get_items_from_db(client)


if __name__ == '__main__':
    main()

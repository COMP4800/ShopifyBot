from connection import get_items_from_db
# This is the initialization of this repo

list_of_clients = [
    'EightXDeveloperTestTable'
]


def main():
    for client in list_of_clients:
        get_items_from_db(client)


if __name__ == '__main__':
    main()

"""
This file will only contain the queries that will be used in getting the data.
"""


# This is the query for the Bulk Operation
def create_bulk_query(start_date_of_orders, end_date_of_orders):
    return f'''
    mutation {{
      bulkOperationRunQuery(
        query:"""
        {{
      orders(query: "created_at:>={start_date_of_orders} AND created_at:<{end_date_of_orders}") {{
        edges {{
          cursor
          node {{
            id
            name  
            customer {{
              id
              numberOfOrders
              averageOrderAmountV2{{amount}}
              createdAt
              orders{{
                edges{{
                    node{{
                        createdAt
                        name
                        displayFulfillmentStatus
                    }}    
                }}
              }}
            }}
            createdAt
            currentTotalDiscountsSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalRefundedSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentSubtotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalShippingPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalRefundedShippingSet{{
                shopMoney{{
                    amount
                }}
            }}
            totalTaxSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
          }}
        }}
      }}
    }}
        """
      ) {{
        bulkOperation {{
          id
          status
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    '''


# This is the query for polling the Bulk Query
PollQuery = '''
    query{
        currentBulkOperation{
            id
            status
            errorCode
            createdAt
            completedAt
            objectCount
            fileSize
            url
            partialDataUrl
        }
    }
'''


# This query is used to cancel an ongoing query
def get_cancel_query(bulk_operation_id: str):
    CancelQuery = f'''
    mutation {{
      bulkOperationCancel(id: "gid://shopify/BulkOperation/{bulk_operation_id}") {{
        bulkOperation {{
          status
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    '''
    return CancelQuery

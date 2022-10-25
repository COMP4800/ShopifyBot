"""
This file will only contain the queries that will be used in getting the data.
"""


# This is the query for the Bulk Operation
BulkOperation = '''
mutation {
  bulkOperationRunQuery(
    query:"""
    {
  orders(query: "created_at:>=2022-09-01 AND created_at:<=2022-09-02") {
    edges {
      cursor
      node {
        customer {
          id
          numberOfOrders
        }
        id
        name
        createdAt
        currentSubtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        cartDiscountAmountSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalShippingPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalTaxSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
      }
    }
  }
}
    """
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
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

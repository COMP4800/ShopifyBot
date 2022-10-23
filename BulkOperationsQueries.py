BulkOperation = '''
mutation {
  bulkOperationRunQuery(
    query:"""
    {
  orders(query: "created_at:>=2022-09-01 AND created_at:<=2022-09-30") {
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

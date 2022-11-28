from connection import wrapper
from apscheduler.schedulers.background import BlockingScheduler
scheduler = BlockingScheduler()


def main():
    scheduler.add_job(id="ScheduledTask", func=wrapper, trigger='cron', day_of_week='mon-sun', hour=0, minute=17)
    scheduler.start()


if __name__ == '__main__':
    main()


# def get_data(url: str) -> list:
#     """
#     This function returns the actual data parsed from the url we get from the Bulk Operation
#     :param url: a String
#     :return: a dictionary
#     """
#     res = requests.get(url)
#     jsonObj = pandas.read_json(json.loads(json.dumps(res.text)), lines=True)
#     json_file = json.loads(jsonObj.to_json(orient='table'))
#     # print(json_file["data"])
#     print(url)
#     data_to_be_pushed = []
#     for each_line in json_file["data"]:
#         line = {}
#         if each_line['id'] is None:
#             line["OrderID"] = ""
#         else:
#             line["OrderID"] = each_line['id']
#
#         if each_line['name'] is None:
#             line["OrderName"] = ""
#         else:
#             line["OrderName"] = each_line['name']
#
#         if each_line['createdAt'] is None:
#             line["OrderDate"] = ""
#         else:
#             date_for_each_order = str(each_line['createdAt'])[:-1]
#             line["OrderDate"] = (datetime.datetime.fromisoformat(date_for_each_order) - timedelta(hours=8)).isoformat()
#             line["Year"] = f'{datetime.datetime.fromisoformat(line["OrderDate"]).year}'
#
#         if each_line['customer'] is None:
#             line["CustomerID"] = ""
#             line["TotalOrdersMadeByTheCustomer"] = ""
#             line["AverageOrderValue"] = ""
#             line["FirstOrderDate"] = ""
#             line["IsFirstOrderMonth"] = "None"
#         else:
#             line["CustomerID"] = each_line['customer']['id']
#             line["TotalOrdersMadeByTheCustomer"] = each_line['customer']['numberOfOrders']
#             line["AverageOrderValue"] = each_line['customer']['averageOrderAmountV2']['amount']
#             line["FirstOrderDate"] = each_line['customer']['createdAt']
#             order_date_month_year = f'{datetime.datetime.fromisoformat(str(each_line["createdAt"])[:-1]).year}-{datetime.datetime.fromisoformat(str(each_line["createdAt"])[:-1]).month}'
#             first_order_month_year = f'{datetime.datetime.fromisoformat(str(each_line["customer"]["createdAt"])[:-1]).year}-{datetime.datetime.fromisoformat(str(each_line["customer"]["createdAt"])[:-1]).month}'
#             if order_date_month_year == first_order_month_year:
#                 line["IsFirstOrderMonth"] = "True"
#             else:
#                 line["IsFirstOrderMonth"] = "False"
#
#         if each_line["currentTotalDiscountsSet"] is None:
#             line["Discounts"] = "0.00"
#         else:
#             line["Discounts"] = each_line["currentTotalDiscountsSet"]['shopMoney']['amount']
#
#         if each_line["totalRefundedSet"] is None:
#             line["Returns"] = "0.00"
#         else:
#             line["Returns"] = each_line["totalRefundedSet"]['shopMoney']['amount']
#
#         if each_line["currentSubtotalPriceSet"] is None:
#             line["NetSales"] = "0.00"
#         else:
#             line["NetSales"] = each_line["currentSubtotalPriceSet"]['shopMoney']['amount']
#
#         if each_line["totalShippingPriceSet"] is None:
#             line["Shipping"] = "0.00"
#         else:
#             line["Shipping"] = each_line["totalShippingPriceSet"]['shopMoney']['amount']
#             if each_line["totalRefundedShippingSet"] is not None:
#                 line["Shipping"] = str(float(each_line["totalShippingPriceSet"]['shopMoney']['amount']) -
#                                        float(each_line["totalRefundedShippingSet"]['shopMoney']['amount']))
#
#         if each_line["totalTaxSet"] is None:
#             line["Taxes"] = "0.00"
#         else:
#             line["Taxes"] = each_line["totalTaxSet"]['shopMoney']['amount']
#
#         if each_line["currentTotalPriceSet"] is None:
#             line["TotalSales"] = "0.00"
#         else:
#             line["TotalSales"] = each_line["currentTotalPriceSet"]['shopMoney']['amount']
#
#         if each_line['currentSubtotalPriceSet'] is None:
#             line["GrossSales"] = "0.00"
#         else:
#             line["GrossSales"] = str(float(line["NetSales"]) +
#                                      float(line["Discounts"]) +
#                                      float(line["Returns"]))
#
#         # print(line)
#         data_to_be_pushed.append(line)
#     return data_to_be_pushed

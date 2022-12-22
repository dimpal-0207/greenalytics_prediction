import pandas as pd
import numpy as np
# https://www.tutorialspoint.com/time_series

df = pd.read_csv('my_pred_data')
# df
curr_year = '2022'
from pymongo import MongoClient

client = MongoClient("mongodb+srv://kamalsherma:l2GIQc5mMOu0gtDo@cluster0.bpyxs.mongodb.net/greenalytics-testing-db")
print("===", client.list_database_names())
db = client['greenalytics-testing-db']
#
# df['Value'] = df['Value'].astype(str)
dict_data ={'categoryId':'', 'locationId':'', 'category':'', 'typeOfData': '', 'companyId':'', 'subCategory1': '', 'subCategory2':'', 'subCategory3':''}
new_list = []
for i, j in df.iterrows():
    new_list.append(j)
    #     print("====>i",i)
    #     print("+++++")
    #     print("=====>j", j)
# print("===>newlist", new_list)

list1 = []
cat = {}
month = {}
for data, value in df.iterrows():
    # print("====>list1", list1)
    # a = [x for x in list1 if all([x['categoryId'] == value.categoryId , x['locationId'] == value.locationId, x['companyId'] == value.companyId , x['category'] == value.category,x['subCategory1'] == value.subCategory1,x['subCategory2'] == value.subCategory2,x['subCategory3'] == value.subCategory3, x['typeOfData'] == value.typeOfData,x['year1'] == value.year1  ])]
    # print("__x", a)
    data_object = -1
    for index, ele in enumerate(list1):
        if ele['categoryId'] == value['categoryId'] and ele['locationId']== value['locationId'] and  ele['companyId'] == value['companyId'] and ele['category'] == value['category'] and ele['subCategory1'] == value['subCategory1']and ele['subCategory2'] == value['subCategory2'] and ele['subCategory3'] == value['subCategory3'] and  ele['typeOfData'] == value['typeOfData'] and ele['year1'] == value['year1'] :
            data_object = index
            # print("+++++>count", index)
            # print("=====",ele)
            # print("data_object", data_object)


    if data_object == -1:
        cat = {**value}
        month = {}
        month[value.month] = value.Value
        # print("___>", month)
        cat['months'] = month

        # print("____cat", cat)
        list1.append(cat)


    else:
        list1[data_object]['months'][value.month] = value.Value
        # print("=====>l1", list1[data_object])
        # print("__a", data_object)  # list of all elements with .n==30
print('++++>list1', list1)
result = db.predict.insert_many(list1)

# print("___result", result)

for  val in list1:
    # print("____val", val)
    # query = {'categoryId': val['categoryId'],
    #          'locationId': val['locationId'],
    #          'companyId': val['companyId'], 'category': val['category'],
    #          'subCategory1': val['subCategory1'],
    #          'subCategory2': val['subCategory2'],
    #          'subCategory3': val['subCategory3'],
    #          'typeOfData': val['typeOfData'], 'year1': val['year1']},
    # update=  {'$set': {'months': val['months']}}
    # upsert = True
    update_database = db.predict.update_one({'categoryId': val['categoryId'],
             'locationId': val['locationId'],
             'companyId': val['companyId'], 'category': val['category'],
             'subCategory1': val['subCategory1'],
             'subCategory2': val['subCategory2'],
             'subCategory3': val['subCategory3'],
             'typeOfData': val['typeOfData'], 'year1': val['year1']},
                {'$set': {'months': val['months']}},
                upsert = True)

    print("====update database and create", update_database)


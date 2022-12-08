#  Copyright (c) 2022. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Authors: Michael A Bouzinier
#           Zifan Gu
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import json
import sys
from decimal import Decimal
import pandas as pd

from psycopg2.extras import RealDictCursor

from nsaph.db import Connection

from pandas import DataFrame

######Enter your query here######
from icd_queries_individual import get_outcomes, get_cust_icd_by_age


#################################

def query(db_ini_file: str, db_conn_name: str, SQL):
    connection = Connection(db_ini_file,
                            db_conn_name,
                            silent=True,
                            app_name_postfix=".sample_query")
    
    with connection.connect() as cnxn:
        with cnxn.cursor(cursor_factory=RealDictCursor) as cursor:
            print("Query in progress...")
            cursor.execute(SQL)
            records = cursor.fetchall()
            print("Converting returned query to dataframe...")
            df = pd.DataFrame([i.copy() for i in records])
            print("Returned {:d} rows".format(len(df)))

            return df


if __name__ == '__main__':
#   
#     age = [[0, 18], [0, 12], [13, 18]]
    age = [[0, 18]] # temp, exploratory
    outcomes = get_outcomes('icd_custom.json')
    custom_icd = outcomes['icd_cust_range']['icd9']
    
    for sub_age in age:
        age_low = sub_age[0]
        age_high = sub_age[1]
        
        
        SQL = get_cust_icd_by_age(age_low, age_high, custom_icd)

        print(SQL)
        df_test = query(sys.argv[1], sys.argv[2], SQL)
        df_test.to_csv(f"/n/dominici_nsaph_l3/Lab/projects/medicaid_children_icd/data/individual_records/test_icd.csv", index=False)
   
                

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
    age = [[0, 18], [0, 12], [13, 18]]
#     age = [[0, 18]] # temp, exploratory
    
    for sub_age in age:
        age_low = sub_age[0]
        age_high = sub_age[1]
        SQL = f'''
        SELECT COUNT(*) AS total_hosp FROM 
    (
    SELECT DISTINCT ON (bene_id) -- choose only unique bene_id on their earliest admission_Date (matching ORDER BY clause)
      bene_id, admission_date, diag, age  FROM 
    (
    SELECT diagnosis[1] as diag, year, admission_date, ad.bene_id, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    )
    AS all_diag
    -- filter for age
    WHERE age IS NOT NULL AND age >= {age_low} AND age <= {age_high}  

    ORDER BY bene_id, admission_date ASC
    ) AS counts
    '''
        print(SQL)
        df_test = query(sys.argv[1], sys.argv[2], SQL)
        df_test.to_csv(f"/n/dominici_nsaph_l3/Lab/projects/medicaid_children_icd/data/icd_290_319/first_hosp/primary_icd_one_number_290_319_age_{age_low}_{age_high}.csv", index=False)
   
                

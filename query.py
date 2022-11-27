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
  SELECT * FROM 
  (
    SELECT diag, COUNT(diag) AS diag_count  FROM 
      (
      SELECT DISTINCT ON (bene_id) -- choose only unique bene_id on their earliest admission_Date (matching ORDER BY clause)
              bene_id, admission_date, diag, age  FROM 
        (
          SELECT unnest(diagnosis[2:]) as diag, year, admission_date, ad.bene_id, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
          INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
        )
        AS all_diag
        -- filter for age
        WHERE age IS NOT NULL AND age >= {age_low} AND age <= {age_high} AND diag IN
        -- filter for diagnosis
        ('290', '2900', '2901', '29010', '29011', '29012', '29013', '2902', '29020', '29021', '2903', '2904', '29040', '29041', '29042', '29043', '2908', '2909', '291', '2910', '2911', '2912', '2913', '2914', '2915', '2918', '29181', '29182', '29189', '2919', '292', '2920', '2921', '29211', '29212', '2922', '2928', '29281', '29282', '29283', '29284', '29285', '29289', '2929', '293', '2930', '2931', '2938', '29381', '29382', '29383', '29384', '29389', '2939', '294', '2940', '2941', '29410', '29411', '2942', '29420', '29421', '2948', '2949', '295', '2950', '29500', '29501', '29502', '29503', '29504', '29505', '2951', '29510', '29511', '29512', '29513', '29514', '29515', '2952', '29520', '29521', '29522', '29523', '29524', '29525', '2953', '29530', '29531', '29532', '29533', '29534', '29535', '2954', '29540', '29541', '29542', '29543', '29544', '29545', '2955', '29550', '29551', '29552', '29553', '29554', '29555', '2956', '29560', '29561', '29562', '29563', '29564', '29565', '2957', '29570', '29571', '29572', '29573', '29574', '29575', '2958', '29580', '29581', '29582', '29583', '29584', '29585', '2959', '29590', '29591', '29592', '29593', '29594', '29595', '296', '2960', '29600', '29601', '29602', '29603', '29604', '29605', '29606', '2961', '29610', '29611', '29612', '29613', '29614', '29615', '29616', '2962', '29620', '29621', '29622', '29623', '29624', '29625', '29626', '2963', '29630', '29631', '29632', '29633', '29634', '29635', '29636', '2964', '29640', '29641', '29642', '29643', '29644', '29645', '29646', '2965', '29650', '29651', '29652', '29653', '29654', '29655', '29656', '2966', '29660', '29661', '29662', '29663', '29664', '29665', '29666', '2967', '2968', '29680', '29681', '29682', '29689', '2969', '29690', '29699', '297', '2970', '2971', '2972', '2973', '2978', '2979', '298', '2980', '2981', '2982', '2983', '2984', '2988', '2989', '299', '2990', '29900', '29901', '2991', '29910', '29911', '2998', '29980', '29981', '2999', '29990', '29991', '300', '3000', '30000', '30001', '30002', '30009', '3001', '30010', '30011', '30012', '30013', '30014', '30015', '30016', '30019', '3002', '30020', '30021', '30022', '30023', '30029', '3003', '3004', '3005', '3006', '3007', '3008', '30081', '30082', '30089', '3009', '301', '3010', '3011', '30110', '30111', '30112', '30113', '3012', '30120', '30121', '30122', '3013', '3014', '3015', '30150', '30151', '30159', '3016', '3017', '3018', '30181', '30182', '30183', '30184', '30189', '3019', '302', '3020', '3021', '3022', '3023', '3024', '3025', '30250', '30251', '30252', '30253', '3026', '3027', '30270', '30271', '30272', '30273', '30274', '30275', '30276', '30279', '3028', '30281', '30282', '30283', '30284', '30285', '30289', '3029', '303', '3030', '30300', '30301', '30302', '30303', '3039', '30390', '30391', '30392', '30393', '304', '3040', '30400', '30401', '30402', '30403', '3041', '30410', '30411', '30412', '30413', '3042', '30420', '30421', '30422', '30423', '3043', '30430', '30431', '30432', '30433', '3044', '30440', '30441', '30442', '30443', '3045', '30450', '30451', '30452', '30453', '3046', '30460', '30461', '30462', '30463', '3047', '30470', '30471', '30472', '30473', '3048', '30480', '30481', '30482', '30483', '3049', '30490', '30491', '30492', '30493', '305', '3050', '30500', '30501', '30502', '30503', '3051', '3052', '30520', '30521', '30522', '30523', '3053', '30530', '30531', '30532', '30533', '3054', '30540', '30541', '30542', '30543', '3055', '30550', '30551', '30552', '30553', '3056', '30560', '30561', '30562', '30563', '3057', '30570', '30571', '30572', '30573', '3058', '30580', '30581', '30582', '30583', '3059', '30590', '30591', '30592', '30593', '306', '3060', '3061', '3062', '3063', '3064', '3065', '30650', '30651', '30652', '30653', '30659', '3066', '3067', '3068', '3069', '307', '3070', '3071', '3072', '30720', '30721', '30722', '30723', '3073', '3074', '30740', '30741', '30742', '30743', '30744', '30745', '30746', '30747', '30748', '30749', '3075', '30750', '30751', '30752', '30753', '30754', '30759', '3076', '3077', '3078', '30780', '30781', '30789', '3079', '308', '3080', '3081', '3082', '3083', '3084', '3089', '309', '3090', '3091', '3092', '30921', '30922', '30923', '30924', '30928', '30929', '3093', '3094', '3098', '30981', '30982', '30983', '30989', '3099', '310', '3100', '3101', '3102', '3108', '31081', '31089', '3109', '311', '312', '3120', '31200', '31201', '31202', '31203', '3121', '31210', '31211', '31212', '31213', '3122', '31220', '31221', '31222', '31223', '3123', '31230', '31231', '31232', '31233', '31234', '31235', '31239', '3124', '3128', '31281', '31282', '31289', '3129', '313', '3130', '3131', '3132', '31321', '31322', '31323', '3133', '3138', '31381', '31382', '31383', '31389', '3139', '314', '3140', '31400', '31401', '3141', '3142', '3148', '3149', '315', '3150', '31500', '31501', '31502', '31509', '3151', '3152', '3153', '31531', '31532', '31534', '31535', '31539', '3154', '3155', '3158', '3159', '316', '317', '318', '3180', '3181', '3182', '319')
        -- GROUP BY diag
        ORDER BY bene_id, admission_date ASC
      ) AS cust_diag_first_hosp
      GROUP BY diag
      ORDER BY COUNT(diag) DESC
    ) AS counts
  WHERE diag_count > 10; -- avoid disclosing counts <=10 per CMS guideline
    '''
        print(SQL)
        df_test = query(sys.argv[1], sys.argv[2], SQL)
    
        df_test.to_csv(f"data/icd_290_319/first_hosp/secondary_icd_290_319_age_{age_low}_{age_high}.csv", index=False)
                

import json



def get_outcomes(filename):
    """ Get and return ICD codes """""
    f = open(filename)
    outcomes_ = json.load(f)
    f.close()
    return json.loads(outcomes_[0])


def get_disease_classification_sql_str(icd_json):
    """
    composes the SQL statement used for classifing disease
    
    icd_json: the JSON file containing keys and values of ICD codes
    return: the CASE WHEN statements that classifies ICD codes into diseases
    """
    
    outcomes = get_outcomes(icd_json)
    
    disease_str_list = []
    # going through the json file
    for key in outcomes['icd_cust_range'].keys():
        if key == 'icd9_all': continue # skips the comprehensive ICD. Handled separatly
        
        # get the list of ICD of that disease
        diseases = outcomes['icd_cust_range'][key]
        
        # create the diag_string (goes into the array filtering in PSQL)
        diag_string = ','.join(diseases)
        disease_str_list.append(f"CASE WHEN diagnosis && '{{{diag_string}}}' THEN 1 ELSE 0 END AS {key}\n")
       
    # join the sentences with comma
    disease_str = ",".join(disease_str_list)
    
    return disease_str
          

def get_cust_icd_by_age(age_low, age_high, diagnoses):
    """
    extracts individual records within the age group, classify disease by ICD, and returns demographics, diagnosis 1-9, year of admission, admission date, beneficiary ID, 
    :param age_low: lower bound of the population age
    :param age_high: upper bound of the population age
    :param diagnoses: a list of ICD codes to match the Medicaid diagnoses
    
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    # separate the diagnosis out
    ind_diag = [f"diagnosis[{i+1}] as diag{i+1}" for i in range(9)]
    ind_diag_str = ",".join(ind_diag)
    
    # create the diag_string (goes into the array filtering in PSQL)
    diag_string = ",".join(diagnoses)
    
    # write the CASE WHEN statements
    case_when_statements = get_disease_classification_sql_str('../icd_custom_Dec12_2022.json')
    
    sql_query = f'''
        SELECT {ind_diag_str}, * FROM 
        (
        -- admission stuff
          SELECT diagnosis, ad.year, admission_date, ad.bene_id, 
        count(*) over (partition by ad.bene_id order by admission_date) as n_th_admission,
        
        --disease classification
        {case_when_statements},
        
        -- demogrpahics
          enroll.zip, race_ethnicity_code, sex, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age          
          FROM medicaid.admissions AS ad
          
          INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
          INNER JOIN medicaid.enrollments AS enroll ON ad.bene_id = enroll.bene_id
          
          -- only want enrollment records of amdission year
          WHERE ad.year = enroll.year
          
          -- filter for diagnosis
          AND diagnosis && '{{{diag_string}}}'
        )
        AS all_diag
        -- filter for age
        WHERE age IS NOT NULL AND age >= {age_low} and age <= {age_high}
        -- GROUP BY diag
        ORDER BY bene_id, admission_date ASC
    '''
    
    return sql_query

def main():
    outcomes = get_outcomes('../icd_custom_Dec12_2022.json')
    custom_icd = outcomes['icd_cust_range']['icd9_all']
    print(get_cust_icd_by_age(0, 18, custom_icd))
    
if __name__ == "__main__":
    main()

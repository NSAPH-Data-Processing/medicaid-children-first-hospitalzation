import json



def get_outcomes(filename):
    """ Get and return ICD codes """""
    f = open(filename)
    outcomes_ = json.load(f)
    f.close()
    return json.loads(outcomes_[0])


def get_cust_icd_by_age(age_low, age_high, diagnoses):
    """
    extracts individual records within the age group
    :param age_low: lower bound of the population age
    :param age_high: upper bound of the population age
    :param diagnoses: a list of ICD codes to match the Medicaid diagnoses
    
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    diag_string = ",".join(diagnoses)
    sql_query = f'''
        SELECT diagnosis[1] as diag1, diagnosis[2] as diag2, diagnosis[3] as diag3, * FROM 
        (
        -- admission stuff
          SELECT diagnosis, ad.year, admission_date, ad.bene_id, 
        count(*) over (partition by ad.bene_id order by admission_date) as n_th_admission,
        
        --disease classification
        CASE WHEN diagnosis && '{{34691,27651}}' THEN 1 ELSE 0 END AS depression,
        CASE WHEN diagnosis && '{{31381, 29620}}' THEN 1 ELSE 0 END AS anxiety,
        
        -- demogrpahics
          enroll.zip, race_ethnicity_code, sex, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age          
          FROM medicaid.admissions AS ad
          
          INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
          INNER JOIN medicaid.enrollments AS enroll ON ad.bene_id = enroll.bene_id
          
          -- only want enrollment records of amdission year
          where ad.year = enroll.year
          limit 10000
        )
        AS all_diag
        -- filter for age
        WHERE age IS NOT NULL AND age >= {age_low} and age <= {age_high}
        -- filter for diagnosis
        AND diagnosis && '{{{diag_string}}}'
        -- GROUP BY diag
        ORDER BY bene_id, admission_date ASC
    '''
    
    return sql_query

def main():
    outcomes = get_outcomes('icd_custom.json')
    custome_icd = outcomes['icd_cust_range']['icd9']

    print(get_cust_icd_by_age(0, 18, custome_icd))
    
if __name__ == "__main__":
    main()

This is a data request made by Dr. Antonella Zanobetti.

## Main request:

*A frequency count of all the ICD codes in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18.*
    
## Follow up request:
A frequency count of ICD codes between 290 and 319 in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18

## Data descriptors:

location: `/n/dominici_nsaph_l3/Lab/projects/medicaid_children_icd/data`

Each CSV file is in descending order of the ICD codes count. Filenames indicate whether the counts came from 1) all ICD codes, 2) primary diagnosis only, or 3) secondary diagnosis only. In addition, the two numbers after `age` indicate the age range. E.g., `primary_icd_age_0_12.csv` contains counts of primary ICD codes in descending order for beneficiaries whose age are between 0 and 12, inclusively.

## Data methods:

Frequency of all ICD codes are extracted by `unnest()` in PSQL build-in functions. To get primary/secondary diagnosis, `diagnosis` column are type `ARRAY` and therefore can be indexed. The order of ICD codes within each array innately indicate primary dignosis, secondary diagnosis, and so on.

```python
def get_freq_icd_counts(age_low, age_high):
    """
    returns a table containing columns: 1) ICD diagnosis code, 2) counts of that code within the age range
    :param age_low: lower bound of the population age, inclusive
    :param age_high: upper bound of the population age, inclusive
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    
    sql_query = f"""
    SELECT diag, COUNT(diag)  FROM 
        (SELECT unnest(diagnosis) as diag, year, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
        INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
        )
        AS all_diag
        WHERE diag IS NOT NULL AND age >= {age_low} and age <= {age_high}
        GROUP BY diag
        ORDER BY COUNT(diag) DESC
        LIMIT 20;
    """
    
    return sql_query


def get_freq_primary_icd_counts(age_low, age_high):
    """
    returns a table containing columns: 1) ICD diagnosis code, 2) counts of that code within the age range
    :param age_low: lower bound of the population age, inclusive
    :param age_high: upper bound of the population age, inclusive
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    
    sql_query = f"""
        SELECT primary_diag, COUNT(primary_diag)  FROM 
        (SELECT diagnosis[1] as primary_diag, year, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
        INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
        )
        AS all_diag
        WHERE age IS NOT NULL AND age >= {age_low} and age <= {age_high}
        GROUP BY primary_diag
        ORDER BY COUNT(primary_diag) DESC
        LIMIT 20;
    """
    
    return sql_query
```
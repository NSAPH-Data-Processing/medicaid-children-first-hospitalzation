This is a data request made by Dr. Antonella Zanobetti.

## Main request:

*A frequency count of all the ICD codes in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18.*
    
## Follow up request:
A frequency count of ICD codes between 290 and 319 in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18

## Data descriptors:

location: `/n/dominici_nsaph_l3/Lab/projects/medicaid_children_icd/data`

Each CSV file is in descending order of the ICD codes count. Filenames indicate whether the counts came from 1) all ICD codes, 2) primary diagnosis only, or 3) secondary diagnosis only. In addition, the two numbers after `age` indicate the age range. E.g., `primary_icd_age_0_12.csv` contains counts of primary ICD codes in descending order for beneficiaries whose age are between 0 and 12, inclusively.

## Definitions
### Defining first hospitalization
Here we define first hospitalization as the first record per beneficiary in **all** years of Medicaid records. This is achieved by logic
```SQL
SELECT DISTINCT ON (bene_id) -- choose only unique bene_id on their earliest admission_Date (matching ORDER BY clause)
  bene_id, admission_date, other_columns FROM
  <...>
  ORDER BY bene_id, admission_date ASC 
```

Note that this means each individual contributes to the count at most **once**. For example, if interested in ICD codes 290-319, and that Patient A presents ICD-9 290.1 on 1/1/2001, and presents ICD-9 315.0 on 6/15/2001, only the hospitalization on 1/1/2001 will be counted; even though on 6/15/2001 was Patient A's first hospitalization for 315.0.

## Data methods:

Frequency of all ICD codes are extracted by `unnest()` in PSQL build-in functions. To get primary/secondary diagnosis, `diagnosis` column are type `ARRAY` and therefore can be indexed. The order of ICD codes within each array innately indicate primary dignosis, secondary diagnosis, and so on.

```python
def get_secondary_freq_icd_counts(age_low, age_high):
    """
    returns a table containing columns: 1) ICD diagnosis code, 2) counts of that code within the age range
    :param age_low: lower bound of the population age, inclusive
    :param age_high: upper bound of the population age, inclusive
    :return: sql_query: the PostgreSQL statement to pass into the engine
    
     Note: for primary ICD, replace `unnest(diagnosis[2:])` to `diagnosis[1]`
    """
    
    sql_query = f"""
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
        (fill_in_your_diagnosis...icd_custome.json is a good place to start)
        -- GROUP BY diag
        ORDER BY bene_id, admission_date ASC
      ) AS cust_diag_first_hosp
      GROUP BY diag
      ORDER BY COUNT(diag) DESC
    ) AS counts
  WHERE diag_count > 10; -- avoid disclosing counts <=10 per CMS guideline
    """
    
    return sql_query
```
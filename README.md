This is a data request made by Dr. Antonella Zanobetti.

## Main request:

*A frequency count of all the ICD codes in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18.*
    
## Follow up request:
A frequency count of ICD codes between 290 and 319 in Medicaid through years 1999-2012, frequency count of primary and secondary on all hospitalizations in kids 0-18 year old. Divided into groups 0-12, and 13-18

## Final request:
On the individual level, extract all beneficiaries with ICD codes of interest, and classify whether the diagnosis is one of the five diseases 
- Major depressive disorder 
- Anxiety 
- Disturbance of emotions specific to childhood 
- Adolescence and adjustment reaction
- Disturbance of conduct 

## Data descriptors:

location: `/n/dominici_nsaph_l3/Lab/projects/medicaid_children_icd/data`

### all_icd_counts
Each CSV file is in descending order of the ICD codes count. Filenames indicate whether the counts came from 1) all ICD codes, 2) primary diagnosis only, or 3) secondary diagnosis only. In addition, the two numbers after `age` indicate the age range. E.g., `primary_icd_age_0_12.csv` contains counts of primary ICD codes in descending order for beneficiaries whose age are between 0 and 12, inclusively.

### icd_290_319
Similar to all_icd_counts, except the ICD is limited to between 290 and 319.

### individual_records
This contains individual level data where individuals diagnosis with ICD extracted from `icd_custom_Dec12_2022.json` is placed into `disease_classification_demographics.csv` with each disease classification (see disease classifications [below](#file-icd_custom_dec12_2022json)). The available columns are:
```
'diag1', 'diag2', 'diag3', 'diag4', 'diag5', 'diag6', 'diag7', 'diag8','diag9', 'diagnosis', 'year', 'admission_date', 'bene_id','n_th_admission', 'depression', 'anxiety', 'emotion_disturb', 'adole_reaction', 'disturb_conduct', 'zip', 'race_ethnicity_code', 'sex', 'age'
```

## Definitions
### Defining admission counts
Variable `n_th_admission` is defined as the number of admissions each beneficiary has been admitted for qualifying ICD codes. That is, if an admission does not contain any of the qualifying ICD codes (for example diagnosis=[100.0, 693.0, NULL, NULL]), that admission record will not be counted.

The counts are NOT grouped by ICD codes. That is, if Patient A presents ICD-9 296.2 only on 12/15/2001, and presents ICD-9 315.0 only on 6/15/2002, the 12/15/2001 visit will be Patient A's first (1) admission, and the 6/15/2002 visit will be Patient A's second (2) admission. Even though on 6/15/2002 was Patient A's first hospitalization for 315.0.

### Defining first hospitalization
**This only applies to analysis done on the counts level, and not the individual level.**

Here we define first hospitalization as the first record per beneficiary in **all** years of Medicaid records. This is achieved by logic
```SQL
SELECT DISTINCT ON (bene_id) -- choose only unique bene_id on their earliest admission_Date (matching ORDER BY clause)
  bene_id, admission_date, other_columns FROM
  <...>
  ORDER BY bene_id, admission_date ASC 
```

Note that this means each individual contributes to the count at most **once**. For example, if interested in ICD codes 290-319, and that Patient A presents ICD-9 290.1 on 1/1/2001, and presents ICD-9 315.0 on 6/15/2001, only the hospitalization on 1/1/2001 will be counted; even though on 6/15/2001 was Patient A's first hospitalization for 315.0.

## File `icd_custom_Dec12_2022.json`
This JSON file contains ICD codes and its keys are
- icd9_all
- depression
- anxiety
- emotion_disturb
- adole_reaction
- disturb_conduct

The classification of these fields are based on the following comments (respective to the fields above)
- *List ICD codes: 296, 299, 300 308,309, 311,312, 313, 314, 315, 317,318, E950-E959*
- *Major depressive disorder 296.2x + 296.3x (can we group 311 with this?)*
- *Anxiety 300.0x to 300.29 + 300.4*
- *Disturbance of emotions specific to childhood (313.0-313.9)*
- *Adolescence and adjustment reaction (309.00 to 309.98)*
- *Disturbance of conduct 312*

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
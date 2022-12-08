import json


def get_outcomes(filename):
    """ Get and return ICD codes """""
    f = open(filename)
    outcomes_ = json.load(f)
    f.close()
    return json.loads(outcomes_[0])


def get_psyc_count(year, age_low, age_high, diagnoses):
    """
    calculates the number of psychiatric diagnoses between age_low and age_high per zip code within a year
    :param year: the year of Medicaid to query
    :param age_low: lower bound of the population age
    :param age_high: upper bound of the population age
    :param diagnoses: a list of ICD codes to match the Medicaid diagnoses
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    diag_string = ",".join(diagnoses)
    sql_query = f"""SELECT * FROM (
    SELECT COUNT(bene_id) AS diag_count, zip  FROM
    (
    SELECT ad.year, ad.bene_id, admission_date, dob, diagnosis, zip, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    INNER JOIN medicaid.enrollments on enrollments.bene_id = ad.bene_id 
    WHERE ad.year = {year} and enrollments.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
    ) 
    AS bene_age -- calculates age
    WHERE age >= {age_low} and age <= {age_high} --column alias cannot be used directly in the query 
    AND diagnosis && '{{{diag_string}}}'
    GROUP BY zip
    ) AS psyc_zip -- calculates psyc diagnosis by zipcode
    """

    return sql_query


def get_hosp_admin_count(year, age_low, age_high):
    """
    calculates the beneficiaries of certain ages per zipcode
    :param year: the year of Medicaid to query
    :param age_low: lower bound of the population age
    :param age_high: upper bound of the population age
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    sql_query = f"""SELECT * FROM (
    SELECT COUNT(bene_id) AS admin_count, zip  FROM
    (
    SELECT ad.year, ad.bene_id, admission_date, dob, diagnosis, zip, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    INNER JOIN medicaid.enrollments on enrollments.bene_id = ad.bene_id 
    WHERE ad.year = {year} and enrollments.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
    ) 
    AS bene_age -- calculates age
    WHERE age >= {age_low} and age <= {age_high} --column alias cannot be used directly in the query 
    GROUP BY zip
    ) AS all_zip -- calculates psyc diagnosis by zipcode
    """

    return sql_query


def get_diag_vs_all_diag(year, age_low, age_high, diagnoses):
    """
    returns a table containing columns: 1)% of diagnoses divided by all admissions 2) diagnoses count 3) total admission count 4) zip code
    :param year: the year of Medicaid to query
    :param age_low: lower bound of the population age
    :param age_high: upper bound of the population age
    :param diagnoses: a list of ICD codes to match the Medicaid diagnoses
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """
    diag_string = ",".join(diagnoses)

    sql_query = f"""
    SELECT diag_count / cast(admin_count AS FLOAT ) AS diag_vs_all_diag, --cast float, otherwise integer division by default
    diag_count, admin_count, all_zip.zip
    FROM 
    (
    
    /* subqery to find all hospital admissions by zipcode */
    SELECT COUNT(bene_id) AS admin_count, zip  FROM
      (
    SELECT ad.year, ad.bene_id, admission_date, dob, diagnosis, zip, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    INNER JOIN medicaid.enrollments on enrollments.bene_id = ad.bene_id 
    WHERE ad.year = {year} and enrollments.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
      ) AS bene_age -- calculates age
    WHERE age >= {age_low} and age <= {age_high} --column alias cannot be used directly in the query 
    GROUP BY zip
    ) AS all_zip -- all hospital admissions
    
    LEFT JOIN (
    
    /* subquery to find admissions identified by ICD codes*/
    SELECT COUNT(bene_id) AS diag_count, zip  FROM
      (
    SELECT ad.year, ad.bene_id, admission_date, dob, diagnosis, zip, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    INNER JOIN medicaid.enrollments on enrollments.bene_id = ad.bene_id 
    WHERE ad.year = {year} and enrollments.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
      ) AS bene_age -- calculates age
    WHERE age >= {age_low} and age <= {age_high} --column alias cannot be used directly in the query 
    AND diagnosis && '{{{diag_string}}}'
    
    GROUP BY zip
    ) as diag_zip -- admissions that have the above diagnoses
    
    ON all_zip.zip = diag_zip.zip -- join by zip code. 
    
    ORDER BY diag_vs_all_diag DESC NULLS last ; --NULL has the largest value in Postgres
    """

    return sql_query


def get_diag_vs_all_enroll(year, age_low, age_high, diagnoses):
    """
    returns a table containing columns: 1)% of diagnoses divided by all enrolls 2) diagnoses count 3) total enrollee count 4) zip code
    :param year: the year of Medicaid to query
    :param age_low: lower bound of the population age, inclusive
    :param age_high: upper bound of the population age, inclusive
    :param diagnoses: a list of ICD codes to match the Medicaid diagnoses
    :return: sql_query: the PostgreSQL statement to pass into the engine
    """

    diag_string = ",".join(diagnoses)

    sql_query = f"""
    SELECT diag_final / cast(enroll_count AS FLOAT ) AS diag_vs_all_enroll, * FROM 
    (
      SELECT  
      coalesce(diag_count, 0) AS diag_final, enroll_count, all_zip.zip -- coalecse replaces all null with 0
      FROM 
      
      /*
      A table that counts the number of beneficiaries by zip code of all beneficiries in a year.
      */
      (
      SELECT COUNT(bene_id) AS enroll_count, zip  FROM
        (
        SELECT enroll.bene_id, dob, year, zip, {year} - EXTRACT(YEAR FROM dob) AS age FROM medicaid.enrollments as enroll
        INNER JOIN medicaid.beneficiaries AS bene ON enroll.bene_id = bene.bene_id 
        WHERE enroll.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
          ) AS bene_age -- calculates age
        WHERE age >= {age_low} and age <= {age_high} -- column alias cannot be used directly in the query 
        GROUP BY zip
      ) AS all_zip -- all enrollments per zipcode
      
      /*
      A table that counts the number of beneficiaries by zip code who have certain ICD diagnosis.
      */ 
      LEFT JOIN (
        SELECT COUNT(bene_id) AS diag_count, zip  FROM
          (
        
        /* Even though admission date and dob can be used to calculate the exact age, using YEAR(dob) - YEAR(of interest) for consistency.*/
        SELECT ad.year, ad.bene_id, admission_date, dob, diagnosis, zip, {year} - EXTRACT(YEAR FROM dob) AS age FROM medicaid.admissions AS ad
        INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
        INNER JOIN medicaid.enrollments on enrollments.bene_id = ad.bene_id 
        WHERE ad.year = {year} and enrollments.year = {year}-- want the year patients is admitted and enrolled for accurate zipcode
          ) AS bene_age -- calculates age
        WHERE age >= {age_low} and age <= {age_high} --column alias cannot be used directly in the query 
        AND diagnosis && '{{{diag_string}}}'
        GROUP BY zip
      ) AS diag_zip -- admissions that have the above diagnoses
      
      ON all_zip.zip = diag_zip.zip -- join by zip code. 
      ) AS diag_w_zeros -- column alias cannot be used directly in the query 
"""
    return sql_query


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
    returns a table containing columns: 1) ICD primary diagnosis code, 2) counts of that code within the age range
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

def get_freq_primary_icd_counts_cust_icd(age_low, age_high):
    """
    generates a query for an age group to find the most frequent diagnoses of the interested ICD codes passed in.
    """

    # ratchet hard-coding because string manipulations (cannot generate quotations for each item and outputted in the print. i.e. print/repr() statements omit quotations)
    sql_query = f"""
        SELECT diag, COUNT(diag)  FROM 
        (SELECT diagnosis[1] as diag, year, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
        INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
        )
        AS all_diag
        WHERE age IS NOT NULL AND age >= {age_low} AND age <= {age_high} AND diag IN
        ('290', '2900', '2901', '29010', '29011', '29012', '29013', '2902', '29020', '29021', '2903', '2904', '29040', '29041', '29042', '29043', '2908', '2909', '291', '2910', '2911', '2912', '2913', '2914', '2915', '2918', '29181', '29182', '29189', '2919', '292', '2920', '2921', '29211', '29212', '2922', '2928', '29281', '29282', '29283', '29284', '29285', '29289', '2929', '293', '2930', '2931', '2938', '29381', '29382', '29383', '29384', '29389', '2939', '294', '2940', '2941', '29410', '29411', '2942', '29420', '29421', '2948', '2949', '295', '2950', '29500', '29501', '29502', '29503', '29504', '29505', '2951', '29510', '29511', '29512', '29513', '29514', '29515', '2952', '29520', '29521', '29522', '29523', '29524', '29525', '2953', '29530', '29531', '29532', '29533', '29534', '29535', '2954', '29540', '29541', '29542', '29543', '29544', '29545', '2955', '29550', '29551', '29552', '29553', '29554', '29555', '2956', '29560', '29561', '29562', '29563', '29564', '29565', '2957', '29570', '29571', '29572', '29573', '29574', '29575', '2958', '29580', '29581', '29582', '29583', '29584', '29585', '2959', '29590', '29591', '29592', '29593', '29594', '29595', '296', '2960', '29600', '29601', '29602', '29603', '29604', '29605', '29606', '2961', '29610', '29611', '29612', '29613', '29614', '29615', '29616', '2962', '29620', '29621', '29622', '29623', '29624', '29625', '29626', '2963', '29630', '29631', '29632', '29633', '29634', '29635', '29636', '2964', '29640', '29641', '29642', '29643', '29644', '29645', '29646', '2965', '29650', '29651', '29652', '29653', '29654', '29655', '29656', '2966', '29660', '29661', '29662', '29663', '29664', '29665', '29666', '2967', '2968', '29680', '29681', '29682', '29689', '2969', '29690', '29699', '297', '2970', '2971', '2972', '2973', '2978', '2979', '298', '2980', '2981', '2982', '2983', '2984', '2988', '2989', '299', '2990', '29900', '29901', '2991', '29910', '29911', '2998', '29980', '29981', '2999', '29990', '29991', '300', '3000', '30000', '30001', '30002', '30009', '3001', '30010', '30011', '30012', '30013', '30014', '30015', '30016', '30019', '3002', '30020', '30021', '30022', '30023', '30029', '3003', '3004', '3005', '3006', '3007', '3008', '30081', '30082', '30089', '3009', '301', '3010', '3011', '30110', '30111', '30112', '30113', '3012', '30120', '30121', '30122', '3013', '3014', '3015', '30150', '30151', '30159', '3016', '3017', '3018', '30181', '30182', '30183', '30184', '30189', '3019', '302', '3020', '3021', '3022', '3023', '3024', '3025', '30250', '30251', '30252', '30253', '3026', '3027', '30270', '30271', '30272', '30273', '30274', '30275', '30276', '30279', '3028', '30281', '30282', '30283', '30284', '30285', '30289', '3029', '303', '3030', '30300', '30301', '30302', '30303', '3039', '30390', '30391', '30392', '30393', '304', '3040', '30400', '30401', '30402', '30403', '3041', '30410', '30411', '30412', '30413', '3042', '30420', '30421', '30422', '30423', '3043', '30430', '30431', '30432', '30433', '3044', '30440', '30441', '30442', '30443', '3045', '30450', '30451', '30452', '30453', '3046', '30460', '30461', '30462', '30463', '3047', '30470', '30471', '30472', '30473', '3048', '30480', '30481', '30482', '30483', '3049', '30490', '30491', '30492', '30493', '305', '3050', '30500', '30501', '30502', '30503', '3051', '3052', '30520', '30521', '30522', '30523', '3053', '30530', '30531', '30532', '30533', '3054', '30540', '30541', '30542', '30543', '3055', '30550', '30551', '30552', '30553', '3056', '30560', '30561', '30562', '30563', '3057', '30570', '30571', '30572', '30573', '3058', '30580', '30581', '30582', '30583', '3059', '30590', '30591', '30592', '30593', '306', '3060', '3061', '3062', '3063', '3064', '3065', '30650', '30651', '30652', '30653', '30659', '3066', '3067', '3068', '3069', '307', '3070', '3071', '3072', '30720', '30721', '30722', '30723', '3073', '3074', '30740', '30741', '30742', '30743', '30744', '30745', '30746', '30747', '30748', '30749', '3075', '30750', '30751', '30752', '30753', '30754', '30759', '3076', '3077', '3078', '30780', '30781', '30789', '3079', '308', '3080', '3081', '3082', '3083', '3084', '3089', '309', '3090', '3091', '3092', '30921', '30922', '30923', '30924', '30928', '30929', '3093', '3094', '3098', '30981', '30982', '30983', '30989', '3099', '310', '3100', '3101', '3102', '3108', '31081', '31089', '3109', '311', '312', '3120', '31200', '31201', '31202', '31203', '3121', '31210', '31211', '31212', '31213', '3122', '31220', '31221', '31222', '31223', '3123', '31230', '31231', '31232', '31233', '31234', '31235', '31239', '3124', '3128', '31281', '31282', '31289', '3129', '313', '3130', '3131', '3132', '31321', '31322', '31323', '3133', '3138', '31381', '31382', '31383', '31389', '3139', '314', '3140', '31400', '31401', '3141', '3142', '3148', '3149', '315', '3150', '31500', '31501', '31502', '31509', '3151', '3152', '3153', '31531', '31532', '31534', '31535', '31539', '3154', '3155', '3158', '3159', '316', '317', '318', '3180', '3181', '3182', '319')
        GROUP BY diag
        ORDER BY COUNT(diag) DESC
        LIMIT 20;
    """
    
    return sql_query
    

    def get_first_hosp_freq_secondary_icd_counts_cust_icd(age_low, age_high):
#     """
#     generates a query for an age group, of their first hospitalization to find the most frequent diagnoses of the interested ICD codes passed in.
    
#     Note: for primary ICD, replace `unnest(diagnosis[2:])` to `diagnosis[1]`
#     """
        sql_query = f'''
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
        return sql_query

    
def get_all_secondary_icd_counts_by_icd(age_low, age_high):
    """
    generates a query for an age group, of their first hospitalization to find the most frequent diagnoses of all ICDs.
    
    Note: for primary ICD, replace `unnest(diagnosis[2:])` to `diagnosis[1]`
    """
        
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
        WHERE age IS NOT NULL AND age >= {age_low} AND age <= {age_high} 
        ORDER BY bene_id, admission_date ASC
      ) AS cust_diag_first_hosp
      GROUP BY diag
      ORDER BY COUNT(diag) DESC
    ) AS counts
  WHERE diag_count > 10; -- avoid disclosing counts <=10 per CMS guideline
    '''
    
    return SQL

def get_all_secondary_icd_counts(age_low, age_high):
    """
    generates a query to find the total count of first hospitalization for an age group
    Returns one integer
    
    Note: for primary ICD, replace `unnest(diagnosis[2:])` to `diagnosis[1]`
    """
    SQL = f'''
        SELECT COUNT(*) AS total_hosp FROM 
    (
    SELECT DISTINCT ON (bene_id) -- choose only unique bene_id on their earliest admission_Date (matching ORDER BY clause)
      bene_id, admission_date, diag, age  FROM 
    (
    SELECT unnest(diagnosis[2:]) as diag, year, admission_date, ad.bene_id, DATE_PART('year', admission_date) - DATE_PART('year', dob) AS age FROM medicaid.admissions AS ad
    INNER JOIN medicaid.beneficiaries AS bene ON ad.bene_id = bene.bene_id 
    )
    AS all_diag
    -- filter for age
    WHERE age IS NOT NULL AND age >= {age_low} AND age <= {age_high}  

    ORDER BY bene_id, admission_date ASC
    ) AS counts
    '''

def main():
    outcomes = get_outcomes('icd_custom.json')
    custome_icd = outcomes['icd_cust_range']['icd9']

    print(get_psyc_count(2008, 0, 18, custome_icd))
    
    # find out what are the keys for this json
#     print(outcomes.keys())
#     custome_icd = outcomes['icd_cust_range']['icd9']
#     print(repr(','.join(custome_icd)))
    
#     icd_str = ",".join(custome_icd)
#     print(icd_str)
#     print('type', type(psyc_icd))
    
#     print(get_freq_primary_icd_counts(0, 18))
    # get_psyc_count(2012, 10, 18, psyc_icd)
#     print(get_psyc_count(2012, 10, 16, custome_icd))
#     print(get_freq_primary_icd_counts_cust_icd(10, 16))
    # print(get_diag_vs_all_diag(2008, 10, 16, psyc_icd))
    # print(get_diag_vs_all_enroll(year=2012, age_low=18, age_high=200, diagnoses=psyc_icd))


if __name__ == "__main__":
    main()

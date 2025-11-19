/*
•	Trimmed and standardized all string fields to uppercase.
•	Derived AdmissionYear and AdmissionMonth for time-based analytics.
•	Calculated TotalDays using datediff(DischargeDate, AdmissionDate).
•	Filtered invalid or inconsistent records (e.g., negative ages, negative costs, invalid date ranges).
•	Added Latest column using window functions for row-level uniqueness.
The transformed data was then written in Delta format to the Silver layer path:
abfss://project6@sreesynapseproject5.dfs.core.windows.net/silver/hospital_clean1
*/
windows_spec=Window.orderBy(col('AdmissionID').desc())

df_clean=df.withColumn('Gender',upper(trim(col('Gender'))))\
    .withColumn('PatientName',upper(trim(col('PatientName'))))\
    .withColumn('Diagnosis',upper(trim(col('Diagnosis'))))\
    .withColumn('Department',upper(trim(col('Department'))))\
    .withColumn('Doctor',upper(trim(col('Doctor'))))\
    .withColumn('HospitalName',upper(trim(col('HospitalName'))))\
    .withColumn('AdmissionYear',year(col('AdmissionDate')))\
    .withColumn('AdmissionMonth',month(col('AdmissionDate')))\
    .withColumn('Latest',row_number().over(windows_spec))\
    .withColumn('TotalDays',datediff(col('DischargeDate'),col('AdmissionDate')))\
    .filter((col('AdmissionDate')<=col('DischargeDate')) & ((col('Age')>0) & (col('TotalCost')>0)))

df_clean.write.format('delta').option("mergeSchema", "true").mode('overwrite').save('abfss://project6@sreesynapseproject5.dfs.core.windows.net/silver/hospital_clean1')


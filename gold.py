dim_patient = silver_df.select("PatientID", "PatientName", "Gender", "Age")\
    .distinct()\
    .withColumn("PatientKey",row_number().over(Window.orderBy("PatientID", "PatientName", "Gender", "Age")))

dim_hospitals=silver_df.select('HospitalName')\
    .distinct()\
    .withColumn('HospitalKey',row_number().over(Window.orderBy(col('HospitalName'))))
  
dim_department=silver_df.select('Department')\
    .distinct()\
    .withColumn('DepartmentKey',row_number().over(Window.orderBy(col('Department'))))
  
dim_diagnosis=silver_df.select('Diagnosis','Department')\
    .distinct()\
    .withColumn('DiagnosisKey',row_number().over(Window.orderBy(col('Diagnosis'))))

dim_doctor = silver_df.select("Doctor", "Department").distinct() \
    .withColumn("DoctorKey", row_number().over(Window.orderBy("Doctor")))


fact_admissions=silver_df.alias('s')\
                .join(dim_patient.alias('p'),
                [
            col("s.PatientID") == col("p.PatientID"),
            col("s.PatientName") == col("p.PatientName"),
            col("s.Gender") == col("p.Gender"),
            col("s.Age") == col("p.Age")
                ],'left'
                )\
                .join(dim_doctor.alias('d'),'Doctor','left')\
                .join(dim_hospitals.alias('h'),'HospitalName','left')\
                .join(dim_department.alias('dept'),'Department','left')\
                .join(dim_diagnosis.alias('dig'),'Diagnosis','left')\
                .select(
                        col('s.AdmissionID'),col('p.PatientKey'),col('dig.DiagnosisKey'),col('dept.DepartmentKey'),
                        col('d.DoctorKey'),col('s.AdmissionDate'),col('s.DischargeDate'),col('h.HospitalKey'),col('s.TotalCost'),
                        col('s.TotalDays'),col('s.AdmissionYear'),col('s.AdmissionMonth')
                )
    

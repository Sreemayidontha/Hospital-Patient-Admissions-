--------------Gold layer---------------------------------
if OBJECT_ID('gold.dim_patient') is not null
    drop EXTERNAL TABLE gold.dim_patient
CREATE EXTERNAL TABLE gold.dim_patient
(
    PatientID VARCHAR(10),
    PatientName VARCHAR(50),
    Gender VARCHAR(10),
    Age INT,
    PatientKey INT
)
WITH(
    location ='hospital_analytics/dim_patient/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)

select * from gold.dim_patient


if OBJECT_ID('gold.dim_hospitals') is not null
    drop EXTERNAL TABLE gold.dim_hospitals
CREATE EXTERNAL TABLE gold.dim_hospitals
(
   HospitalName VARCHAR(50),
    HospitalKey INT
)
WITH(
    location ='hospital_analytics/dim_hospitals/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)
select * from gold.dim_hospitals

if OBJECT_ID('gold.dim_department') is not null
    drop EXTERNAL TABLE gold.dim_department
CREATE EXTERNAL TABLE gold.dim_department
(
   Department VARCHAR(50),
    DepartmentKey INT
)
WITH(
    location ='hospital_analytics/dim_department/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)

select * from gold.dim_department



if OBJECT_ID('gold.dim_diagnosis') is not null
    drop EXTERNAL TABLE gold.dim_diagnosis
CREATE EXTERNAL TABLE gold.dim_diagnosis
(
   Diagnosis VARCHAR(50),
   Department VARCHAR(50),
    DiagnosisKey INT
)
WITH(
    location ='hospital_analytics/dim_diagnosis/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)

select * from gold.dim_diagnosis





if OBJECT_ID('gold.dim_doctor') is not null
    drop EXTERNAL TABLE gold.dim_doctor
CREATE EXTERNAL TABLE gold.dim_doctor
(
   Doctor VARCHAR(50),
   Department VARCHAR(50),
    DoctorKey INT
)
WITH(
    location ='hospital_analytics/dim_doctor/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)

select * from gold.dim_doctor


if OBJECT_ID('gold.fact_admissions') is not null
    drop EXTERNAL TABLE gold.fact_admissions
CREATE EXTERNAL TABLE gold.fact_admissions
(
    AdmissionID VARCHAR(10),
    PatientKey INT,
    DiagnosisKey INT,
    DepartmentKey INT,
    DoctorKey INT,
    AdmissionDate DATE,
    DischargeDate DATE,
    HospitalKey INT,
    TotalCost INT,-- Money or Float not working in serverless since the parquet/delta stored it as int32
    TotalDays INT,
    AdmissionYear INT,
    AdmissionMonth INT
)
WITH(
    location ='hospital_analytics/fact_admissions/',
    DATA_SOURCE=gold_scr,
    FILE_FORMAT=delta_format
)

select * from gold.fact_admissions
select * from gold.dim_patient
select * from gold.dim_department
select * from gold.dim_doctor
select * from gold.dim_hospitals
select * from gold.dim_diagnosis

SELECT *
FROM OPENROWSET(
    BULK 'hospital_analytics/fact_admissions/',
    DATA_SOURCE = 'gold_scr',
    FORMAT = 'DELTA'
) AS rows
WHERE PatientKey IS NOT NULL


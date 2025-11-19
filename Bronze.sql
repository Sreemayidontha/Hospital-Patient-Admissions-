


----Create external table in bronze

if OBJECT_ID('bronze.hospital_all') is NOT NULL
   DROP EXTERNAL TABLE bronze.hospital_all
create EXTERNAL TABLE bronze.hospital_all
   (
      AdmissionID varchar(10),
      PatientID varchar(10),
      PatientName varchar(50),
      Gender varchar(10),
      Age INT,
      Diagnosis varchar(30),
      Department varchar(50),
      Doctor varchar(50),
      AdmissionDate DATE,
      DischargeDate DATE,
      HospitalName varchar(50),
      TotalCost MONEY
   )
   WITH(
      LOCATION ='Healthcare_PatientAdmissions.csv',
      DATA_SOURCE=hospital_src,
      FILE_FORMAT=csv_file_format_pv1,
      REJECT_VALUE=10,
      REJECTED_ROW_LOCATION='rejection/hospital'
   )


select * from bronze.hospital_all


---Create view in bronze

DROP VIEW IF EXISTS bronze.vw_hospital_all
go
CREATE VIEW bronze.vw_hospital_all 
AS SELECT * from bronze.hospital_all

---------------------------------------------------------------------


select top 10 * from bronze.vw_hospital_all 

select * from bronze.vw_hospital_all where AdmissionDate > '2025-11-08' -- should retrive none

ALTER view bronze.vw_hospital_all 
AS
select 
AdmissionID,
    PatientID,
    PatientName,
    Gender,
    Age,
    Diagnosis,
    Department,
    Doctor,
    AdmissionDate, 
    DischargeDate,
    HospitalName,
    TotalCost,
    DATEDIFF(day,AdmissionDate,DischargeDate) as TotalDays 
FROM bronze.hospital_all;



select top 10 * from bronze.vw_hospital_all 

select count(*)from bronze.vw_hospital_all

select count(DISTINCT(Doctor)) from bronze.vw_hospital_all--9

select count(DISTINCT(HospitalName)) from bronze.vw_hospital_all--6
select count(DISTINCT(Diagnosis)) from bronze.vw_hospital_all


select count(DISTINCT(PatientID)) from bronze.vw_hospital_all --59

select count(distinct(Department))from bronze.vw_hospital_all --9

select count(DISTINCT(AdmissionID)) from bronze.vw_hospital_all --300

select Doctor , count(HospitalName) as [No.of_hospitals] FROM bronze.vw_hospital_all
GROUP BY Doctor
order by [No.of_hospitals] DESC

select * from bronze.vw_hospital_all where AdmissionDate > DischargeDate -- retrived none

SELECT 
    PatientID, 
    COUNT(DISTINCT Age) AS AgeCount, 
    COUNT(DISTINCT Gender) AS GenderCount
FROM bronze.vw_hospital_all
GROUP BY PatientID
HAVING (COUNT(DISTINCT Age) > 1
    OR COUNT(DISTINCT Gender) > 1);


select * from bronze.vw_hospital_all where PatientID='P002'

-- 9 different departments 9 different doctors

select Doctor, COUNT(DISTINCT Department) FROM bronze.vw_hospital_all GROUP by Doctor HAVING count(distinct Department)>1 -- No rows
--each doctor work in only one department

--If same patient have different PatientId for different admissions

select PatientID,PatientName
from bronze.vw_hospital_all 
GROUP BY PatientID,PatientName,Age,Gender
HAVING count(PatientID) >1

-- How many times each patient got admitted

SELECT PatientID, PatientName, COUNT(*) AS NumAdmissions
FROM bronze.vw_hospital_all
GROUP BY PatientID, PatientName,Age, Gender
HAVING COUNT(*) > 1;     ---------------------------------No patient admitted more than once.



select * from bronze.vw_hospital_all where PatientID='P002'



select Doctor,Department, count(AdmissionID) as Total_no_patients from bronze.vw_hospital_all group by Doctor,Department order by count(AdmissionID) DESC

--Most admitted hospital/loyal hospital

select HospitalName, COUNT(AdmissionID) [Total_Admissions] from bronze.vw_hospital_all group by HospitalName order by COUNT(AdmissionID) DESC
--CityCare Hospital


SELECT 

    distinct Diagnosis,
    AVG(TotalDays) OVER(PARTITION BY Diagnosis) AS AvgDaysForDiagnosis
FROM bronze.vw_hospital_all;




SELECT Diagnosis, Avg(TotalCost) as Average_days_spent FROM bronze.vw_hospital_all group by Diagnosis order by Avg(TotalCost) DESC



select HospitalName, SUM(TotalCost) as [Total_income_generated] from bronze.vw_hospital_all group by HospitalName order by SUM(TotalCost) DESC
--CityCare Hospital


SELECT Diagnosis, Avg(TotalDays) as Average_days_spent FROM bronze.vw_hospital_all group by Diagnosis order by Avg(TotalDays) DESC

--Most expensive patient by department

with ranked as
(select PatientID, PatientName, Diagnosis, TotalCost,
 rank() over(partition by Diagnosis order by TotalCost Desc) AS CostRank
FROM bronze.vw_hospital_all) select * from ranked where CostRank =1


-----Validation for silver layer-----
--1. Ensure AdmissionDate < DischargeDate
select * from bronze.vw_hospital_all where AdmissionDate > '2025-11-08' --  retrived none
select * from bronze.vw_hospital_all where AdmissionDate > DischargeDate -- retrived none

--2.Drop rows with missing key fields (AdmissionID, PatientID..)
select * from bronze.vw_hospital_all where PatientID = NULL or PatientID= ''
select count(*)from bronze.vw_hospital_all

select count(DISTINCT(AdmissionID)) from bronze.vw_hospital_all --300

select * from bronze.vw_hospital_all where Age<0
select * from bronze.vw_hospital_all where TotalCost <0

select top 10* from bronze.vw_hospital_all                                                                                                                                                                              

select *, ROW_NUMBER() over(order by AdmissionDate  desc) as Latest from  bronze.vw_hospital_all

----------------------------------------------------------
/*
if OBJECT_ID(silver.hospital_clean) is not NULL
 drop  TABLE silver.hospital_clean
 go
create  TABLE  silver.hospital_clean
WITH(
   LOCATION ='hospital_clean1',
   DATA_SOURCE= silver_scr,

    FILE_FORMAT = parquet_format
)
as select
AdmissionID,
    PatientID,
    PatientName,
    UPPER(LTRIM(RTRIM(Gender))) AS Gender,
    Age,
    Diagnosis,
    Department,
    Doctor,
    AdmissionDate,
    DischargeDate,
    HospitalName,
    TotalCost,
    DATEDIFF(DAY, AdmissionDate, DischargeDate) AS TotalDays,
    YEAR(AdmissionDate) AS AdmissionYear,
    MONTH(AdmissionDate) AS AdmissionMonth,
    ROW_NUMBER() over(order by AdmissionDate  desc) as Latest
FROM bronze.vw_hospital_all
WHERE AdmissionDate <= DischargeDate
  AND Age > 0
  AND TotalCost > 0;
  */
------------------------------------------------------
select * from openrowset(
    bulk 'hospital_clean1',
    DATA_SOURCE='silver_scr',
    FORMAT = 'DELTA'
)as [result]

--Create External table for the silver schema-------------
if OBJECT_ID('silver.hospital_clean') is not null
    drop EXTERNAL TABLE silver.hospital_clean 
CREATE EXTERNAL TABLE silver.hospital_clean
(
    AdmissionID VARCHAR(10),
    PatientID VARCHAR(10),
    PatientName VARCHAR(50),
    Gender VARCHAR(10),
    Age INT,
    Diagnosis VARCHAR(50),
    Department VARCHAR(50),
    Doctor VARCHAR(50),
    AdmissionDate DATE,
    DischargeDate DATE,
    HospitalName VARCHAR(50),
    TotalCost INT,-- Money or Float not working in serverless since the parquet/delta stored it as int32
    TotalDays INT,
    AdmissionYear INT,
    AdmissionMonth INT
)
WITH (
    LOCATION = 'hospital_clean1',
    DATA_SOURCE = silver_scr,
    FILE_FORMAT = delta_format
);
 
select * from silver.hospital_clean
select * from silver.hospital_clean where AdmissionID='A1299'

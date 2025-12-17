-- Refined v3: safe integer parsing (round), TEXT for large text columns, ROW_FORMAT=DYNAMIC
SET @percent_as_fraction = TRUE;

DROP TABLE IF EXISTS `staging_hr_analytics_import`;
CREATE TABLE `staging_hr_analytics_import` (
  `id` TEXT,
  `empid` TEXT,
  `age` TEXT,
  `agegroup` TEXT,
  `attrition` TEXT,
  `businesstravel` TEXT,
  `dailyrate` TEXT,
  `department` TEXT,
  `distancefromhome` TEXT,
  `education` TEXT,
  `educationfield` TEXT,
  `employeecount` TEXT,
  `employeenumber` TEXT,
  `environmentsatisfaction` TEXT,
  `gender` TEXT,
  `hourlyrate` TEXT,
  `jobinvolvement` TEXT,
  `joblevel` TEXT,
  `jobrole` TEXT,
  `jobsatisfaction` TEXT,
  `maritalstatus` TEXT,
  `monthlyincome` TEXT,
  `salaryslab` TEXT,
  `monthlyrate` TEXT,
  `numcompaniesworked` TEXT,
  `over18` TEXT,
  `overtime` TEXT,
  `percentsalaryhike` TEXT,
  `performancerating` TEXT,
  `relationshipsatisfaction` TEXT,
  `standardhours` TEXT,
  `stockoptionlevel` TEXT,
  `totalworkingyears` TEXT,
  `trainingtimeslastyear` TEXT,
  `worklifebalance` TEXT,
  `yearsatcompany` TEXT,
  `yearsincurrentrole` TEXT,
  `yearssincelastpromotion` TEXT,
  `yearswithcurrmanager` TEXT,
  `managertenuremissing` TEXT,
  `attritionflag` TEXT,
  `tenuregroup` TEXT,
  `ishighearner` TEXT,
  `longcommute` TEXT,
  `salarymin` TEXT,
  `salarymax` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

DROP TABLE IF EXISTS `hr_analytics_final_cleaned`;
CREATE TABLE `hr_analytics_final_cleaned` (
  `import_id` INT NOT NULL AUTO_INCREMENT,
  `id` VARCHAR(100) DEFAULT NULL,
  `empid` VARCHAR(100) DEFAULT NULL,
  `age` INT DEFAULT NULL,
  `agegroup` INT DEFAULT NULL,
  `attrition` TEXT DEFAULT NULL,
  `businesstravel` TEXT DEFAULT NULL,
  `dailyrate` TEXT DEFAULT NULL,
  `department` TEXT DEFAULT NULL,
  `distancefromhome` TEXT DEFAULT NULL,
  `education` TEXT DEFAULT NULL,
  `educationfield` TEXT DEFAULT NULL,
  `employeecount` INT DEFAULT NULL,
  `employeenumber` INT DEFAULT NULL,
  `environmentsatisfaction` TEXT DEFAULT NULL,
  `gender` TEXT DEFAULT NULL,
  `hourlyrate` TEXT DEFAULT NULL,
  `jobinvolvement` TEXT DEFAULT NULL,
  `joblevel` TEXT DEFAULT NULL,
  `jobrole` TEXT DEFAULT NULL,
  `jobsatisfaction` TEXT DEFAULT NULL,
  `maritalstatus` TEXT DEFAULT NULL,
  `monthlyincome` TEXT DEFAULT NULL,
  `salaryslab` DECIMAL(18,4) DEFAULT NULL,
  `monthlyrate` TEXT DEFAULT NULL,
  `numcompaniesworked` INT DEFAULT NULL,
  `over18` TEXT DEFAULT NULL,
  `overtime` TEXT DEFAULT NULL,
  `percentsalaryhike` DECIMAL(18,4) DEFAULT NULL,
  `performancerating` TEXT DEFAULT NULL,
  `relationshipsatisfaction` TEXT DEFAULT NULL,
  `standardhours` TEXT DEFAULT NULL,
  `stockoptionlevel` TEXT DEFAULT NULL,
  `totalworkingyears` DECIMAL(18,4) DEFAULT NULL,
  `trainingtimeslastyear` TEXT DEFAULT NULL,
  `worklifebalance` DECIMAL(18,4) DEFAULT NULL,
  `yearsatcompany` TEXT DEFAULT NULL,
  `yearsincurrentrole` TEXT DEFAULT NULL,
  `yearssincelastpromotion` TEXT DEFAULT NULL,
  `yearswithcurrmanager` INT DEFAULT NULL,
  `managertenuremissing` INT DEFAULT NULL,
  `attritionflag` TEXT DEFAULT NULL,
  `tenuregroup` TEXT DEFAULT NULL,
  `ishighearner` TEXT DEFAULT NULL,
  `longcommute` TEXT DEFAULT NULL,
  `salarymin` DECIMAL(18,4) DEFAULT NULL,
  `salarymax` DECIMAL(18,4) DEFAULT NULL
  ,PRIMARY KEY (`import_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

LOAD DATA LOCAL INFILE 'C:/Users/Sanket/Project_Data_Analysis/hr_analytics_import_full_package/HR_Analytics_Final_MySQL_Ready_final_cleaned.csv'
INTO TABLE `staging_hr_analytics_import`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`id`, `empid`, `age`, `agegroup`, `attrition`, `businesstravel`, `dailyrate`, `department`, `distancefromhome`, `education`, `educationfield`, `employeecount`, `employeenumber`, `environmentsatisfaction`, `gender`, `hourlyrate`, `jobinvolvement`, `joblevel`, `jobrole`, `jobsatisfaction`, `maritalstatus`, `monthlyincome`, `salaryslab`, `monthlyrate`, `numcompaniesworked`, `over18`, `overtime`, `percentsalaryhike`, `performancerating`, `relationshipsatisfaction`, `standardhours`, `stockoptionlevel`, `totalworkingyears`, `trainingtimeslastyear`, `worklifebalance`, `yearsatcompany`, `yearsincurrentrole`, `yearssincelastpromotion`, `yearswithcurrmanager`, `managertenuremissing`, `attritionflag`, `tenuregroup`, `ishighearner`, `longcommute`, `salarymin`, `salarymax`);

INSERT INTO `hr_analytics_final_cleaned` (`id`, `empid`, `age`, `agegroup`, `attrition`, `businesstravel`, `dailyrate`, `department`, `distancefromhome`, `education`, `educationfield`, `employeecount`, `employeenumber`, `environmentsatisfaction`, `gender`, `hourlyrate`, `jobinvolvement`, `joblevel`, `jobrole`, `jobsatisfaction`, `maritalstatus`, `monthlyincome`, `salaryslab`, `monthlyrate`, `numcompaniesworked`, `over18`, `overtime`, `percentsalaryhike`, `performancerating`, `relationshipsatisfaction`, `standardhours`, `stockoptionlevel`, `totalworkingyears`, `trainingtimeslastyear`, `worklifebalance`, `yearsatcompany`, `yearsincurrentrole`, `yearssincelastpromotion`, `yearswithcurrmanager`, `managertenuremissing`, `attritionflag`, `tenuregroup`, `ishighearner`, `longcommute`, `salarymin`, `salarymax`)
SELECT
  NULLIF(TRIM(`id`),'') AS `id`,
  NULLIF(TRIM(`empid`),'') AS `empid`,
  CASE WHEN TRIM(`age`) = '' OR `age` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`age`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `age`,
  CASE WHEN TRIM(`agegroup`) = '' OR `agegroup` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`agegroup`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `agegroup`,
  NULLIF(TRIM(`attrition`),'') AS `attrition`,
  NULLIF(TRIM(`businesstravel`),'') AS `businesstravel`,
  NULLIF(TRIM(`dailyrate`),'') AS `dailyrate`,
  NULLIF(TRIM(`department`),'') AS `department`,
  NULLIF(TRIM(`distancefromhome`),'') AS `distancefromhome`,
  NULLIF(TRIM(`education`),'') AS `education`,
  NULLIF(TRIM(`educationfield`),'') AS `educationfield`,
  CASE WHEN TRIM(`employeecount`) = '' OR `employeecount` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`employeecount`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `employeecount`,
  CASE WHEN TRIM(`employeenumber`) = '' OR `employeenumber` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`employeenumber`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `employeenumber`,
  NULLIF(TRIM(`environmentsatisfaction`),'') AS `environmentsatisfaction`,
  NULLIF(TRIM(`gender`),'') AS `gender`,
  NULLIF(TRIM(`hourlyrate`),'') AS `hourlyrate`,
  NULLIF(TRIM(`jobinvolvement`),'') AS `jobinvolvement`,
  NULLIF(TRIM(`joblevel`),'') AS `joblevel`,
  NULLIF(TRIM(`jobrole`),'') AS `jobrole`,
  NULLIF(TRIM(`jobsatisfaction`),'') AS `jobsatisfaction`,
  NULLIF(TRIM(`maritalstatus`),'') AS `maritalstatus`,
  NULLIF(TRIM(`monthlyincome`),'') AS `monthlyincome`,
  CASE WHEN TRIM(`salaryslab`)='' OR `salaryslab` IS NULL THEN NULL ELSE CAST(REPLACE(REPLACE(REPLACE(`salaryslab`,'$',''),',',''),'%' , '') AS DECIMAL(18,4)) END AS `salaryslab`,
  NULLIF(TRIM(`monthlyrate`),'') AS `monthlyrate`,
  CASE WHEN TRIM(`numcompaniesworked`) = '' OR `numcompaniesworked` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`numcompaniesworked`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `numcompaniesworked`,
  NULLIF(TRIM(`over18`),'') AS `over18`,
  NULLIF(TRIM(`overtime`),'') AS `overtime`,
  CASE WHEN TRIM(`percentsalaryhike`)='' OR `percentsalaryhike` IS NULL THEN NULL WHEN @percent_as_fraction THEN CAST(REPLACE(`percentsalaryhike`,'%','') AS DECIMAL(10,4))/100 ELSE CAST(REPLACE(`percentsalaryhike`,'%','') AS DECIMAL(10,4)) END AS `percentsalaryhike`,
  NULLIF(TRIM(`performancerating`),'') AS `performancerating`,
  NULLIF(TRIM(`relationshipsatisfaction`),'') AS `relationshipsatisfaction`,
  NULLIF(TRIM(`standardhours`),'') AS `standardhours`,
  NULLIF(TRIM(`stockoptionlevel`),'') AS `stockoptionlevel`,
  CASE WHEN TRIM(`totalworkingyears`)='' OR `totalworkingyears` IS NULL THEN NULL ELSE CAST(REPLACE(REPLACE(REPLACE(`totalworkingyears`,'$',''),',',''),'%' , '') AS DECIMAL(18,4)) END AS `totalworkingyears`,
  NULLIF(TRIM(`trainingtimeslastyear`),'') AS `trainingtimeslastyear`,
  CASE WHEN TRIM(`worklifebalance`)='' OR `worklifebalance` IS NULL THEN NULL WHEN @percent_as_fraction THEN CAST(REPLACE(`worklifebalance`,'%','') AS DECIMAL(10,4))/100 ELSE CAST(REPLACE(`worklifebalance`,'%','') AS DECIMAL(10,4)) END AS `worklifebalance`,
  NULLIF(TRIM(`yearsatcompany`),'') AS `yearsatcompany`,
  NULLIF(TRIM(`yearsincurrentrole`),'') AS `yearsincurrentrole`,
  NULLIF(TRIM(`yearssincelastpromotion`),'') AS `yearssincelastpromotion`,
  CASE WHEN TRIM(`yearswithcurrmanager`) = '' OR `yearswithcurrmanager` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`yearswithcurrmanager`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `yearswithcurrmanager`,
  CASE WHEN TRIM(`managertenuremissing`) = '' OR `managertenuremissing` IS NULL THEN NULL ELSE CAST( ROUND( CAST( REPLACE(`managertenuremissing`, ',', '') AS DECIMAL(18,4) ), 0 ) AS SIGNED ) END AS `managertenuremissing`,
  NULLIF(TRIM(`attritionflag`),'') AS `attritionflag`,
  NULLIF(TRIM(`tenuregroup`),'') AS `tenuregroup`,
  NULLIF(TRIM(`ishighearner`),'') AS `ishighearner`,
  NULLIF(TRIM(`longcommute`),'') AS `longcommute`,
  CASE WHEN TRIM(`salarymin`)='' OR `salarymin` IS NULL THEN NULL ELSE CAST(REPLACE(REPLACE(REPLACE(`salarymin`,'$',''),',',''),'%' , '') AS DECIMAL(18,4)) END AS `salarymin`,
  CASE WHEN TRIM(`salarymax`)='' OR `salarymax` IS NULL THEN NULL ELSE CAST(REPLACE(REPLACE(REPLACE(`salarymax`,'$',''),',',''),'%' , '') AS DECIMAL(18,4)) END AS `salarymax`
FROM `staging_hr_analytics_import`;

-- Suggested indexes (commented)
/*
CREATE INDEX idx_age ON `hr_analytics_final_cleaned` (`age`);
CREATE INDEX idx_empid ON `hr_analytics_final_cleaned` (`empid`);
*/

SELECT COUNT(*) AS staging_rows FROM `staging_hr_analytics_import`;
SELECT COUNT(*) AS final_rows FROM `hr_analytics_final_cleaned`;

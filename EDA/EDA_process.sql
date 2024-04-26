-- First overview of Data  
SELECT *
FROM layoffs;

/* First Insight
- Null & blank values present 
- Date is a string format, change to Datetime 
(can change easily to datetime format when loading the data in MySQL, kept it for practice reasons)
- Remove any columns if needed after further analysis 
- Company columns has entries with spaces in the front of the word
*/

-- Create Staging tables to preserve original data 
CREATE TABLE layoffs_staging
LIKE layoffs; 


-- Check new staging table (should be empty)
SELECT *
FROM layoffs_staging;  


-- Insert data into staging table from layoffs
INSERT layoffs_staging
SELECT * 
FROM layoffs;


-- Check staging table is filled 
SELECT *
FROM layoffs_staging; 

-- Now that staging table is complete thorough data clenaing can start
	-- Duplicated
    -- removing columns 
    -- Standardization 
    
-- Duplicates: This dataset has no unique identifier 
/*
Make a unique identifier with all the rows to identify which entries have duplicates using Partition by for each column.
This will be a cte and the row_num will idetntify if that row is unique or not.  
*/
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1; -- 5 rows appear to be duplicates 

-- Create staging2 table to delete row_num >1

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int -- add row_num 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Check staging2 table (should be empty)
SELECT *
FROM layoffs_staging2;

-- Insert from layoffs_staging into layoffs_staging2 
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Check layoffs_staging2 has the data inserted, Filter row_num > 1 
SELECT * 
FROM layoffs_staging2 
WHERE row_num >1;

-- Delete duplicates / row_num > 1 
SET SQL_SAFE_UPDATES = 0;
DELETE
FROM layoffs_staging2
WHERE row_num >1;
SET SQL_SAFE_UPDATES = 1;

-- Check table doe snto have row_num >1 
SELECT * 
FROM layoffs_staging2 
WHERE row_num >1; -- result is 0 

-- Standardizing data 
	-- company name was spacing differences 
SELECT company, TRIM(company)   
FROM layoffs_staging2; 
    
-- Update table with TRIM(company)
SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET company = TRIM(company);
SET SQL_SAFE_UPDATES = 1;

-- Investigate industry columns: crypto, nulls and blanks should be handled  
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;  

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; -- various naming methods used

-- Update all crypto currency variation to = 'Crypto'
Update layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
 
-- All changes have been made to crypto industry
SELECT DISTINCT industry 
FROM layoffs_staging2; 

-- Country column 
SELECT DISTINCT(country)
FROM layoffs_staging2; 
 
-- United States has 2 naming methods
Update layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Verify it changed to 'United States'
SELECT * 
FROM layoffs_staging2
WHERE country LIKE 'United States';

-- Change date text column datatype to datetime

SELECT `date`
FROM layoffs_staging2;
-- String to Date 
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

-- Alter table to date format 
ALTER TABLE layoffs_staging2
MODIFY COLUMN  `date` DATE; 

-- Review date column 
SELECT * 
FROM layoffs_staging2;

/*
Next, handle the following in various columns
- Blank values 
- Null values 
*/

-- Find same company columns with blank values and fill them in 
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 

-- Check blank values are handled 
SELECT company, industry
FROM layoffs_staging2;


/*
Unsuccessful: Did not work changing blanks from t1 to t2  with method above 
Next approach: Change the blank values to NULL and then join the tables from t1. to t2 for industry 
*/ 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; 

-- Check blanks changed to NULL 
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL or t1.industry = '')
AND t2.industry IS NOT NULL;

-- Update table from NULL and fill NULL with industry for same company name 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

-- Review it worked 
SELECT *
FROM layoffs_staging2 
WHERE industry IS NULL;

/*
Bally's Interactive did not change
After research it was found that Bally's Interactive is in the gaming industry
Update the industry to gaming 
*/

-- Update to Gaming 
UPDATE layoffs_staging2
SET industry = 'Gaming'
WHERE industry IS NULL;

-- Review changes 
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'gaming%';


-- Review other columns with missing values 
SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


/*
Deleting total laid off and percentage laid off are NULL
Keeping this data would effect the EDA
*/ 

-- Delete NULLs 
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Review columns
SELECT *
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Drop row_num column as it doesn't add any value (also to the EDA process)
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


/*
First Data cleaning process has been completed 
*/ 


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*
Exploratory Data Analysis 

Explore each column to identify patterns in the data and answer questions regarding:

- When did these layoffs occur?
- What industry has had the most layoffs?
- Dates of layoffs (is there a particular pattern for the timing of layoffs?)
- What industry is least affected by layoffs? 
- How does this vary by country?
- Does funding influence if layoffs increase or decrease? 

*/ 
-- Pull up data for Review 
SELECT *
FROM layoffs_staging2;

-- Date range 
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- Date ranges from 2020 - 2023, covid could have been a important factor to wide spread layoffs 



-- Max total layoffs 
-- 12,000 total with 100% laid off (company went under)
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Explore companies 
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
/*
Major global companies have laid off over 10,000 employees 
Amazon	18150
Google	12000
Meta	11000
Salesforce	10090
Microsoft	10000
Philips	10000

lowest laid off are 35 employees
*/


SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

/*
Top 5 industries effected by layoffs are 
Consumer - 45182
Retail - 43613
Other - 36289
Transportation - 33748
Finance - 28344
*/

-- By country 
-- America has the highest total more than all other countries combined in the dataset 
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY country
ORDER BY 2 DESC;  

-- Layoffs by date for United States 
SELECT YEAR(`date`),country, SUM(total_laid_off)
FROM layoffs_staging2 
WHERE country = 'United States'
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;  

-- Date and total for all countries 
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;  
/*
2022 had the highest layoffs, 2021 was the lowest.   
However for 2023 we only have 3 montsh worth of data but the total is 125,677 total layoffs already 
*/

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2 
GROUP BY YEAR(`date`)
ORDER BY 2 DESC; 

-- Stage of companies with the most layoffs 
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;
-- Post IPO had the most with 204,132
-- Subsidary & SEED had the least

-- Total Layoffs with each month throughout the years (ignore NULLS)
SELECT substring(`date`,1,7) AS `Month`, sum(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC; 

-- There are a total of 383,159 (383,659 layoffs including nulls). How is this spread across by months, and years?  
SELECT sum(total_laid_off)
FROM layoffs_staging2; 

/*
Create a rolling total of layoffs to show each month and year to see how it adds to the total amount  
*/

WITH Rolling_Total AS
( 
SELECT substring(`date`,1,7) AS `Month`, sum(total_laid_off) As total_layoffs
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_layoffs
, sum(total_layoffs) OVER (ORDER BY `Month`) As rolling_total 
FROM Rolling_Total;
-- Year 2022 had the worst layoffs comparative to the the from 96821 to 257482 rolling total 


-- Can include percentage column to better understand the impact of layoffs for a certain month and year 
WITH Rolling_Total AS
( 
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `Month`, 
        SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `Month`
    ORDER BY 1 ASC
)
SELECT 
    `Month`, 
    total_layoffs,
    ROUND((total_layoffs / SUM(total_layoffs) OVER ()) * 100, 2) AS percentage_of_total,
    SUM(total_layoffs) OVER (ORDER BY `Month`) AS rolling_total 
FROM Rolling_Total;
-- 2023-01 had the highest percentage of layoffs around the world

-- Which country had the highest layoffs 
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
-- America had the majority of layoffs ( 256559 out of 383,659 = 67% total ) 

-- Which company laid off the most by year 
SELECT company, YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

/*
So far most layoffs come from America within the large companies Amazon, Google, Meta and Microsoft.   
2023 had the highest layoff percentage despite it only have 3 months recorded in that year. 
*/

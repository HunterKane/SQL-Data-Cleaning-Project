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
SET industry = 'gaming'
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
Data cleaning has been completed 
*/ 
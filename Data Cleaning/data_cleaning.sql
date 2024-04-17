-- First overview of Data  
SELECT *
FROM layoffs;

/* First Insight
- Null & blank values present 
- Date is a string format, change to Datetime 
(can change easily to datetime format when loading the data in MySQL, kept it for practice reasons)
- Remove any columns if needed after further analysis 
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


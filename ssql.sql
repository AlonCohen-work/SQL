-- SQL Project - Data Cleaning

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary
SELECT * 
FROM world_layoffs.layoffs;

create table layoffs_stag
like layoffs;

SELECT * 
FROM layoffs_stag;

insert layoffs_stag
SELECT * 
FROM layoffs;

-- count change in table if there where change in the PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
with duplicate_cte as
(
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stag
)
SELECT * 
FROM duplicate_cte
WHERE row_num> 1;

SELECT * 
FROM layoffs_stag
WHERE company= 'Oda';

CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_stag;


DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;

-- 2. standardize data
SELECT company, trim(company)
FROM layoffs_staging2;

update layoffs_staging2
set company= trim(company);

SELECT distinct industry
FROM layoffs_staging2
order by 1;

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%' ;



update layoffs_staging2
set industry = 'Crypto' 
where industry like 'Crypto%';



SELECT distinct country ,trim(Trailing '.' from country)
FROM layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(Trailing '.' from country)
where country like 'United States%' ;

SELECT `date`,
str_to_date( `date`,'%m/%d/%Y')
FROM layoffs_staging2
order by 1;

update layoffs_staging2
set `date` = str_to_date( `date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;


-- 3. Look at null values
SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


SELECT *
FROM layoffs_staging2
where industry is null
or industry = '' 
order by 1;

SELECT *
FROM layoffs_staging2
where company = 'Airbnb' ;


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry ='')
and t2.industry is not null ;


update layoffs_staging2 
set industry = null
where industry = '' ;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null ;

delete
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- 4. remove any columns and rows that are not necessary
SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


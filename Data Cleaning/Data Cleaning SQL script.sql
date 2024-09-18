/*https://www.kaggle.com/datasets/swaptr/layoffs-2022?resource=download*/

/*Data Cleaning MySQL*/

SELECT *
FROM layoffs ;

Select Count(*)
From layoffs ;

/*Create mock up data for work to protect the raw data*/
create table layoffs_staging
like layoffs ;

insert layoffs_staging
select *
from layoffs ;

/*1. Remove Duplicate*/

-- Check dup.
with dup_cte as(
select *,
	   row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from dup_cte
where row_num > 1 ;

/*
SELECT *
FROM layoffs
where company = 'casper' ;
*/

-- layoffs_staging_redup = new table with no dup.
CREATE TABLE `layoffs_staging_redup` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ;

insert into layoffs_staging_redup
select *,
	   row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging ;

-- Remove duplicates.
delete
from layoffs_staging_redup
where row_num > 1 ;

-- Recheck duplicates.
select *
from layoffs_staging_redup
where row_num > 1 ;


/*2. Standardize Data*/

/*
SELECT date
FROM layoffs_staging_redup 
order by 1 asc ;
*/

-- Remove spaces.
update layoffs_staging_redup
set company = trim(company);

-- Crypto has multiple different variations. So, Change crypto industry to the same format. 
update layoffs_staging_redup
set industry = 'Crypto'
where industry like 'Crypto%' ;

-- There are 'united states' and 'united states'. So, Remove '.'
update layoffs_staging_redup
set country = trim(trailing '.' from country)
where country like 'united states%' ;

-- String to date.
update layoffs_staging_redup
set `date` = str_to_date(`date`, '%m/%d/%Y') ;

-- Convert the data type.
alter table layoffs_staging_redup
modify column `date` date ;


/*Null Values or Blank*/

-- Check null or blank in industry column.
select *
from layoffs_staging_redup
where industry is null or industry = '' ;

-- Sampling Airbnb and I see both null and not null. Check it again after fill null.
select *
from layoffs_staging_redup
where company = 'Airbnb' ;

-- Convert Blank to null value 
update layoffs_staging_redup
set industry = null
where industry = '' ;

-- Fill null. 
update layoffs_staging_redup t1
join layoffs_staging_redup t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null ;

/*
select *
from layoffs_staging_redup
*/

/*Remove Rows and Columns*/

select * 
from layoffs_staging_redup
where total_laid_off is null
and percentage_laid_off is null ;

-- Remove the null data from those columns because I think if both columns are null, It is useless for analysis.
delete
from layoffs_staging_redup
where total_laid_off is null
and percentage_laid_off is null ;

alter table layoffs_staging_redup
drop column row_num ;

select * 
from layoffs_staging_redup






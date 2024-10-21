/*Exploratory Data Analysis*/

select *
from layoffs_staging_redup ; 

-- Looking at max laid off by total and percentage.
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging_redup ; 

-- Finding the companies that have shut down by looking for the percentage laid off = 1.
-- Checking company size by the total number of employees laid off.
select *
from layoffs_staging_redup
where percentage_laid_off = 1 
order by total_laid_off desc ;

-- by the total funds.
select *
from layoffs_staging_redup
where percentage_laid_off = 1 
order by funds_raised_millions  desc ;  

-- Companies with the most Total Layoffs
select company, sum(total_laid_off)
from layoffs_staging_redup
group by company
order by 2 desc ;

-- by location
select location, sum(total_laid_off)
from layoffs_staging_redup
group by location
order by 2 desc ;

-- by industry
select industry, sum(total_laid_off)
from layoffs_staging_redup
group by industry
order by 2 desc ;

-- by country
select country, sum(total_laid_off)
from layoffs_staging_redup
group by country
order by 2 desc ;

-- by stage
select stage, sum(total_laid_off)
from layoffs_staging_redup
group by stage
order by 2 desc ;

select min(`date`), max(`date`)
from layoffs_staging_redup ; 

-- by year
select year(`date`), sum(total_laid_off)
from layoffs_staging_redup
group by year(`date`)
order by 1 desc ;

-- by year-month
select substring(`date`, 1, 7) as `Month`, sum(total_laid_off)
from layoffs_staging_redup
where substring(`date`, 1, 7) is not null
group by `Month` 
order by 1 asc ; 

-- Rolling total by year-month
with rolling_total as
(
select substring(`date`, 1, 7) as `Month`, sum(total_laid_off) as total_laid_off
from layoffs_staging_redup
where substring(`date`, 1, 7) is not null
group by `Month` 
order by 1 asc 
)
select `Month`, total_laid_off
		, sum(total_laid_off) over (order by `Month`) as rolling_total
        , sum(total_laid_off) over () as total
from rolling_total ;

-- Top 3 companies by total laid off
with Company_Year as 
(
  select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
  from layoffs_staging_redup
  group by company, year(`date`)
)
, Company_Year_Rank as (
  select company, years, total_laid_off, dense_rank() over (partition by years order by total_laid_off desc) as ranking
  from Company_Year
)
select company, years, total_laid_off, ranking
from Company_Year_Rank
where ranking <= 3
and years is not null
order by years asc, total_laid_off desc ;





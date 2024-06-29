
--                                                                  DATA CLEANING
--                                         1. Remove Duplicates
--                                         2. Standarize the Data
--                                         3. Null values or Blank Values
--                                         4. Remove any columnlayoff_stagginglayoff_stagging
use world_layoffs;
select * from layoffs;

-- We should not work in the raw data. SO create a duplicate table.
create table Layoff_Stagging like layoffs;
select * from layoff_stagging;
delete from layoff_stagging;
Insert layoff_stagging select  * from layoffs;

--                                                            1. Remove Duplicates


     select*,  row_number() over(partition by company, total_laid_off, percentage_laid_off, 'date') as row_num from layoff_stagging;   -- giving row number to rows
     
     WITH duplicate_CTE as
	     (
          select*,  row_number() over(partition by company, total_laid_off, percentage_laid_off,location, industry, stage, country, 'date', funds_raised_millions) as
                   row_num from layoff_stagging
          )
              select * from duplicate_CTE where row_num > 1;      -- Finding the row where row_num > 1.
              
              select * from layoff_stagging where company = 'casper';
              
               -- Again we are creating the separate table for storing the data without duplicates
              
              CREATE TABLE `layoff_stagging2` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_stagging2;

insert  layoff_stagging2
select*,  row_number() over(partition by company, total_laid_off, percentage_laid_off,location, industry, stage, country, 'date', funds_raised_millions) as
                   row_num from layoff_stagging;   -- inserted the data with the row num 
                   
                   delete from layoff_stagging2;
rollback; -- used to undo the sql query

     select * from layoff_stagging2 where row_num > 1; --  finding the duplicates
     
     Delete from layoff_stagging2 where row_num > 1;  -- Deleting the duplicates



                    --                                          2. Standarizing the Data
                    
                    --                          It means finding the issues in your data and fixing it
     -- Trim - removes the space character OR other specified characters from the start or end of a string
     
                 -- Standarizing the Company column
                 
     select company from layoff_stagging2;
     select company, trim(company) from layoff_stagging2;
     
     update layoff_stagging2 
        set company = trim(company) ; -- Updating trim function in company column
        
				 -- Standarizing the Industry column
                 
	  select * from layoff_stagging2;layoff_stagging2
	
   select distinct(industry) from layoff_stagging2 order by 1;    
   
   update layoff_stagging2 set  industry = "crypto"  where industry like "crypto%";  -- updating company name where cryptocurrecy to crypto
   
                   -- Standarizing the Location column
		
   select distinct(country) from layoff_stagging2 order by 1;
   select distinct(country), trim(trailing "." from country) from layoff_stagging2 order by 1;
   
   update layoff_stagging2 set country = trim(trailing "." from country) where country like "United States%";
  
  
                     -- Standarizing the Date column
                     
 select date, str_to_date(`date`, '%m/%d/%Y') from layoff_stagging2;
 
 update layoff_stagging2 set date = str_to_date(`date`, '%m/%d/%Y');  -- updating the date syntax
 
 alter table layoff_stagging2 modify column date DATE;                -- Modified the data type of date from text to DATE




 
  --                                                   3. Null Values or Blank values
  
  select * from layoff_stagging2;
  
  select * from layoff_stagging2 
        where total_laid_off is NULL
         and percentage_laid_off is NULL;    -- total_laid_off NULL's and percentage_laid_off NULL's
	
  select * from layoff_stagging2    
      where industry is NULL or 
      industry = "";                          -- industry NULL's and blank values
      
    select * from layoff_stagging2    
      where company = "Airbnb";                -- checking the same company name has one industry column filled and one not. eg: Airbnb - industry
      
	-- so we are using join and separating the table by the idea of Airbnb
    
update layoff_stagging2 set industry = NULL where industry = "";   -- updating all blank industry values to null

    select * , t1.industry, t2.industry 
       from layoff_stagging2 t1 JOIN
            layoff_stagging2 t2  on
               t1.company = t2.company
                 where(t1.industry is null and t2.industry is not null); -- viewing the row of industry is null
                 
	update layoff_stagging2 t1 JOIN
            layoff_stagging2 t2  on
               t1.company = t2.company
                 set t1.industry = t2.industry
                  where(t1.industry is null and t2.industry is not null); -- updating  the industry fields with the similar fields.  




                  
                     --                               4. Remove any column 
               

SELECT *
FROM layoff_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

                                       -- Delete Useless data we can't really use
         DELETE FROM layoff_stagging2
          WHERE total_laid_off IS NULL
			AND percentage_laid_off IS NULL;
 
                  ALTER TABLE layoff_stagging2
                      DROP COLUMN row_num;          -- Dropping the column row_num

           select * from layoff_stagging2;















                     
                    

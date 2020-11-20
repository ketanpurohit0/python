use role accountadmin;
select system$get_snowflake_platform_info();


use database reviews;

select * from BUSINESSES;

insert into BUSINESSES values ('ID1','K3');

create or replace view B_1 
AS SELECT * FROM BUSINESSES;

create or replace secure view B_1_S
AS SELECT * FROM BUSINESSES;

SELECT * FROM B_1 WHERE 1/iff(BUSINESS_ID = 'IFx', 0,1)  =1;

SELECT * FROM B_1_S WHERE 1/iff(BUSINESS_ID = 'IFx', 0,1)  =1;

select get_ddl('view','B_1_S');


CREATE OR REPLACE STORAGE INTEGRATION awss3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::..:role/mysnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://..','s3://../');
  
desc integration awss3_int;

grant create stage on schema public to role ACCOUNTADMIN;

grant usage on integration awss3_int to role PUBLIC;

-- "REVIEWS"."PUBLIC"."MY_AWSS3_STAGE"
use schema REVIEWS.public;

create or replace file format my_csv_format
  type = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  field_delimiter = ','
  skip_header = 0;
  
show file formats;


create or replace  stage my_awss3_stage
  storage_integration = awss3_int
  url = 's3://../...csv'
  file_format = my_csv_format;
  
-- SQL compilation error: The supplied properties CREDENTIALS and STORAGE_INTEGRATION are incompatible with each other. (1) For credential-based operations, only use CREDENTIALS. (2) For credential-less operations, only use STORAGE_INTEGRATION.

create or replace stage my_awss3_stage2
  storage_integration = awss3_int
  credentials=(aws_key_id='' aws_secret_key='')
  url = 's3://..'
  file_format = my_csv_format;
    
select $1 from @my_awss3_stage;
list @my_awss3_stage;
list @REVIEWS.PUBLIC.my_awss3_stage;

select $1 from @my_awss3_stage2;
list @my_awss3_stage2;
list @REVIEWS.PUBLIC.my_awss3_stage2;
  
desc stage my_awss3_stage; 
  
copy into BUSINESSES from @my_awss3_stage ;

 
copy into BUSINESSES from @my_awss3_stage2 credentials=(aws_key_id='' aws_secret_key='');

pattern='.*sales.*.csv';

-- works

copy into BUSINESSES
  from s3://bdh5pketanpurohit0 credentials=(aws_key_id='' aws_secret_key='')
  file_format = (format_name = my_csv_format);



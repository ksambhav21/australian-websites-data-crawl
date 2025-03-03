Author: Kumar Sambhav (email: ksambhav34@gmail.com)

Approach to building a data pipeline for Australian Company Information: Extraction, Cleaning, and Integration

Step 1: Extract Australian Company Websites from Common Crawl
•	Search for domain URLs like.com.au, net.au, etc. using Common Crawl index.
•	Fetch corresponding WARC records and extract useful details (Company Name, Industry, Emails, Contact Info).
•	Store the extracted website data in a structured format.
Using your provided Python script, we will refine the Common Crawl extraction:
•	Extract company names from titles/meta tags.
•	Detect industries based on keywords.
•	Extract email, phone numbers, and social media links.

Step 2: Fetch Business Data from ABR
•	Query the Australian Business Register (ABR) using its API (or dataset from data.gov.au).
•	Match company names/domains with the ABN and other business details.
•	Clean the data to handle inconsistencies in names and domains.

Step 3: Data Cleaning & Normalization using Pandas and DBT
•	Remove duplicate entries.
•	Normalize company names (handle different spellings/capitalization).
•	Standardize industries, phone numbers, and emails.
•	Merge website and ABR datasets based on URL or business name.

Step 4: Store Processed Data in Postgres
•	Define a schema to store the integrated company data.
•	Load cleaned data into a Postgres database.
•	Create dbt models for transformations, applying indexing for efficient querying.

Step 5: Implement Tests & Permissions in DBT
•	Add primary key, uniqueness, and referential integrity tests in dbt.
•	Ensure the tables are readable using the reader role in Postgres.

Step 6: Querying the Data for Business Insights
•	Example queries:
o	Retrieve all businesses in a given industry.
o	Find companies with missing business registration details.
o	Analyse industry trends based on the collected data.

Schema Design (Postgres DDL)
Tables:
1.	common_crawl_websites: Stores extracted website data.
2.	abr_businesses: Stores Australian Business Register (ABR) data.
SQL: 
CREATE TABLE common_crawl_websites (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    company_name TEXT,
    title TEXT,
    description TEXT,
    industry TEXT,
    emails TEXT,
    phones TEXT,
    social_links TEXT,
    snapshot_date TIMESTAMP
);

CREATE TABLE abr_businesses (
    abn VARCHAR(20) PRIMARY KEY,
    business_name TEXT NOT NULL,
    trading_name TEXT,
    industry TEXT,
    state TEXT,
    postcode TEXT,
    registration_date DATE,
    url TEXT UNIQUE,
    source TEXT DEFAULT 'ABR'
);

-- Create an integrated view of company data
CREATE VIEW company_data AS
SELECT 
    coalesce(cc.company_name, ab.business_name) AS company_name,
    cc.url AS website_url,
    ab.abn,
    coalesce(cc.industry, ab.industry) AS industry,
    cc.emails,
    cc.phones,
    ab.state,
    ab.postcode,
    ab.registration_date
FROM common_crawl_websites cc
LEFT JOIN abr_businesses ab
ON cc.url = ab.url OR LOWER(cc.company_name) = LOWER(ab.business_name);
2. dbt Model: Cleaning & Normalizing Data
The given below model does:
•	Clean company names (removes special characters and trims spaces).
•	Standardize industries.
•	Deduplicate records.
SQL:
WITH website_data AS (
    SELECT
        url,
        LOWER(TRIM(company_name)) AS company_name_clean,
        LOWER(TRIM(industry)) AS industry_clean,
        emails,
        phones,
        snapshot_date
    FROM {{ ref('stg_company_websites') }}  -- Refers to staging table
),

business_data AS (
    SELECT
        abn,
        LOWER(TRIM(company_name)) AS company_name_clean,
        industry AS business_industry,
        website,
        registration_date,
        state
    FROM {{ ref('stg_company_details') }}
)

-- Merge website data with business data
SELECT
    w.url,
    COALESCE(w.company_name_clean, b.company_name_clean) AS company_name,
    w.industry_clean AS detected_industry,
    b.business_industry AS official_industry,
    b.abn,
    b.registration_date,
    b.state,
    w.emails,
    w.phones,
    w.snapshot_date
FROM website_data w
LEFT JOIN business_data b
ON w.company_name_clean = b.company_name_clean


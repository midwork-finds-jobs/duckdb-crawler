-- Job Crawling Pipeline for Zoho Recruit Sites
-- Two-stage crawl: list pages -> detail pages
--
-- Stage 1: Crawl main pages to get job IDs
-- Stage 2: Crawl individual job pages for full details
--
-- Sites: carrieres.os4techno.com, recruit.srilankan.com

LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Create jobs table for storing extracted job data
CREATE TABLE IF NOT EXISTS jobs (
    url VARCHAR,
    id VARCHAR PRIMARY KEY,
    posting_title VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    industry VARCHAR,
    job_type VARCHAR,
    work_experience VARCHAR,
    remote_job BOOLEAN,
    date_opened DATE,
    salary VARCHAR,
    job_description TEXT,
    crawled_at TIMESTAMP DEFAULT current_timestamp
);

-- Two-stage crawl using CTEs with LATERAL join
-- Stage 1: Get job URLs from listing pages
-- Stage 2: Crawl each detail page via LATERAL crawl_url() (supports column references)
STREAM (
    WITH job_urls AS (
        SELECT
            -- rtrim to remove trailing slash before appending path
            format('{}jobs/Careers/{}', rtrim(url, '/') || '/', job->>'id') as job_url
        FROM (
            SELECT url, unnest(jq(html.document, 'input#jobs', 'value')::JSON[]) as job
            FROM crawl(
                ['https://carrieres.os4techno.com/', 'https://recruit.srilankan.com/'],
                user_agent = 'JobCrawler/1.0'
            )
            WHERE status = 200
        )
        WHERE job->>'id' IS NOT NULL
    )
    -- html.js['jobs'] is an array, access first element with [0]
    SELECT
        c.url,
        c.html.js['jobs'][0]->>'id' as id,
        c.html.js['jobs'][0]->>'Posting_Title' as posting_title,
        c.html.js['jobs'][0]->>'City' as city,
        c.html.js['jobs'][0]->>'State' as state,
        c.html.js['jobs'][0]->>'Country' as country,
        c.html.js['jobs'][0]->>'Industry' as industry,
        c.html.js['jobs'][0]->>'Job_Type' as job_type,
        c.html.js['jobs'][0]->>'Work_Experience' as work_experience,
        COALESCE(CAST(c.html.js['jobs'][0]->>'Remote_Job' AS BOOLEAN), false) as remote_job,
        TRY_CAST(c.html.js['jobs'][0]->>'Date_Opened' AS DATE) as date_opened,
        c.html.js['jobs'][0]->>'Salary' as salary,
        c.html.js['jobs'][0]->>'Job_Description' as job_description,
        current_timestamp as crawled_at
    FROM job_urls,
         LATERAL crawl_url(job_url) c
    WHERE c.status = 200 AND c.html.js['jobs'] IS NOT NULL
) INTO jobs WITH (batch_size 50);

-- Show results
SELECT 'Crawl complete!' as status;
SELECT count(*) as total_jobs, count(DISTINCT url) as sources, count(DISTINCT country) as countries FROM jobs;
SELECT id, posting_title, city, country, salary FROM jobs LIMIT 10;

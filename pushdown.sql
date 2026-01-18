-- Job Crawling Pipeline for Zoho Recruit Sites
-- Two-stage crawl: list pages -> detail pages
--
-- Stage 1: Crawl main pages to get job IDs
-- Stage 2: Crawl individual job pages for full details
--
-- Sites: carrieres.os4techno.com, recruit.srilankan.com

LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Create or replace jobs table for crawl results
DROP TABLE IF EXISTS jobs;

-- Two-stage crawl: listing pages -> detail pages via LATERAL crawl_url()
STREAM (
    WITH job_urls AS (
        SELECT url, unnest(htmlpath(html.document, 'input#jobs@value[*]')::JSON[]) as job
        FROM crawl(['https://carrieres.os4techno.com/', 'https://recruit.srilankan.com/'])
        WHERE status = 200
    ),
    job_pages AS (
        SELECT c.url, c.html.js['jobs'][0] as job
        FROM job_urls j, LATERAL crawl_url(format('{}/jobs/Careers/{}', rtrim(j.url,'/'), j.job->>'id')) c
        WHERE c.status = 200 AND c.html.js['jobs'] IS NOT NULL
    )
    SELECT
        url,
        job->>'id' as id,
        job->>'Posting_Title' as posting_title,
        job->>'City' as city,
        job->>'State' as state,
        job->>'Country' as country,
        job->>'Industry' as industry,
        job->>'Job_Type' as job_type,
        job->>'Work_Experience' as work_experience,
        (job->>'Remote_Job')::BOOLEAN as remote_job,
        (job->>'Date_Opened')::DATE as date_opened,
        job->>'Salary' as salary,
        job->>'Job_Description' as job_description
    FROM job_pages
) INTO jobs;

-- Show results
SELECT 'Crawl complete!' as status;
SELECT count(*) as total_jobs, count(DISTINCT url) as sources, count(DISTINCT country) as countries FROM jobs;
SELECT id, posting_title, city, country, salary FROM jobs LIMIT 10;

-- Job Crawling Pipeline for Zoho Recruit Sites
-- Two-stage crawl: list pages -> detail pages
--
-- Stage 1: Crawl main pages to get job IDs
-- Stage 2: Crawl individual job pages for full details
--
-- Sites: carrieres.os4techno.com, recruit.srilankan.com

LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Two-stage crawl: crawl() for listing, LATERAL crawl() for details
WITH job_urls AS (
    SELECT format('{}jobs/Careers/{}', url, job_id) as job_url
    FROM (
        SELECT url, unnest(htmlpath(html.document, 'input#jobs@value[*].id')::BIGINT[]) as job_id
        FROM crawl([
            'https://carrieres.os4techno.com/',
            'https://recruit.srilankan.com/',
            'https://mlhco.zohorecruit.com/'
        ])
        WHERE status = 200
    )
),
job_pages AS (
    SELECT c.url, htmlpath(c.html.document, 'script@$jobs[0]')::JSON as job
    FROM job_urls, LATERAL crawl_url(job_url) c
    WHERE c.status = 200
)
SELECT
    url,
    (job->>'id')::BIGINT as id,
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
LIMIT 2;

-- Show cached requests
SELECT url, status_code, content_type, cached_at
FROM __crawler_cache
ORDER BY cached_at DESC;

-- zoho-local.sql
-- Parse Zoho career page job postings into a structured table
-- Prerequisites: Start local server with zoho-career.html in /tmp:
--   python3 -m http.server 48765 --directory /tmp

-- Load extension (json is autoloaded)
LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Crawl the local zoho career page (LIMIT is pushed down to crawler)
CRAWL (SELECT 'http://localhost:48765/zoho-career.html') INTO zoho_raw
WITH (user_agent 'TestBot/1.0') LIMIT 1;

-- Extract job postings from js->'jobs' array
CREATE OR REPLACE TABLE zoho_jobs AS
SELECT
    job->>'id' as job_id,
    job->>'Posting_Title' as title,
    job->>'Poste' as position,
    job->>'Job_Type' as job_type,
    job->>'Salary' as salary,
    job->>'Currency' as currency,
    job->>'City' as city,
    job->>'State' as state,
    job->>'Country' as country,
    job->>'Zip_Code' as zip_code,
    (job->>'Remote_Job')::BOOLEAN as is_remote,
    job->>'Industry' as industry,
    job->>'Work_Experience' as experience,
    job->>'Date_Opened' as date_opened,
    job->'Langue' as languages,
    regexp_replace(job->>'Job_Description', '<[^>]*>', '', 'g') as description_text,
    job->>'Job_Description' as description_html,
    job->>'Required_Skills' as required_skills,
    (job->>'Publish')::BOOLEAN as is_published,
    zoho_raw.url as source_url,
    zoho_raw.crawled_at
FROM zoho_raw,
LATERAL unnest(from_json(js->'jobs', '["{}"]')) as t(job)
WHERE js->'jobs' IS NOT NULL;

-- Show results
SELECT 'Extracted ' || COUNT(*) || ' job postings' as status FROM zoho_jobs;

SELECT
    job_id,
    title,
    job_type,
    salary,
    city || ', ' || state || ', ' || country as location,
    is_remote,
    experience,
    date_opened
FROM zoho_jobs;

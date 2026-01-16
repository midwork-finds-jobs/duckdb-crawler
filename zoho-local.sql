-- zoho-local.sql
-- Parse Zoho career page job postings into a structured table
-- Prerequisites: Start local server with zoho-career.html in /tmp:
--   python3 -m http.server 48765 --directory /tmp

-- Load extensions
LOAD 'build/release/extension/crawler/crawler.duckdb_extension';
INSTALL json;
LOAD json;

-- Crawl the local zoho career page (LIMIT is pushed down to crawler)
CRAWL (SELECT 'http://localhost:48765/zoho-career.html') INTO zoho_raw
WITH (user_agent 'TestBot/1.0') LIMIT 1;

-- Extract job postings from the js column
-- The jobs variable contains a JSON array of job objects
CREATE OR REPLACE TABLE zoho_jobs AS
WITH jobs_json AS (
    SELECT
        url,
        crawled_at,
        json(js::JSON->>'jobs') as jobs_array
    FROM zoho_raw
    WHERE js IS NOT NULL  -- empty results stored as NULL, not ''
      AND json_valid(js::JSON->>'jobs')
)
SELECT
    json_extract_string(job.j, '$.id') as job_id,
    json_extract_string(job.j, '$.Posting_Title') as title,
    json_extract_string(job.j, '$.Poste') as position,
    json_extract_string(job.j, '$.Job_Type') as job_type,
    json_extract_string(job.j, '$.Salary') as salary,
    json_extract_string(job.j, '$.Currency') as currency,
    json_extract_string(job.j, '$.City') as city,
    json_extract_string(job.j, '$.State') as state,
    json_extract_string(job.j, '$.Country') as country,
    json_extract_string(job.j, '$.Zip_Code') as zip_code,
    COALESCE(json_extract_string(job.j, '$.Remote_Job') = 'true', false) as is_remote,
    json_extract_string(job.j, '$.Industry') as industry,
    json_extract_string(job.j, '$.Work_Experience') as experience,
    json_extract_string(job.j, '$.Date_Opened') as date_opened,
    json_extract(job.j, '$.Langue') as languages,
    -- Clean HTML from description
    regexp_replace(json_extract_string(job.j, '$.Job_Description'), '<[^>]*>', '', 'g') as description_text,
    json_extract_string(job.j, '$.Job_Description') as description_html,
    json_extract_string(job.j, '$.Required_Skills') as required_skills,
    COALESCE(json_extract_string(job.j, '$.Publish') = 'true', false) as is_published,
    jobs_json.url as source_url,
    jobs_json.crawled_at
FROM jobs_json,
LATERAL (SELECT unnest(json_extract(jobs_json.jobs_array, '$[*]')) as j) as job;

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

-- models/question_answer_timeseries.sql

WITH question_answer_times AS (
  SELECT 
    q.creationdate AS question_date,
    a.creationdate AS answer_date,
   -- Calculate the difference in seconds
    EXTRACT(EPOCH FROM (a.creationdate - q.creationdate)) AS time_diff_seconds
  FROM {{ ref('posts') }} q
  JOIN {{ ref('posts') }} a
    ON q.id = a.acceptedanswerid
  WHERE q.posttypeid = 1  -- Only questions
    AND a.posttypeid = 2  -- Only answers
),
time_bands AS (
  SELECT 
    question_date,
    CASE
      WHEN time_diff_seconds < 60 THEN '<1 min'
      WHEN time_diff_seconds BETWEEN 60 AND 300 THEN '1-5 mins'
      WHEN time_diff_seconds BETWEEN 301 AND 3600 THEN '5 mins-1 hour'
      WHEN time_diff_seconds BETWEEN 3601 AND 10800 THEN '1-3 hours'
      WHEN time_diff_seconds BETWEEN 10801 AND 86400 THEN '3 hours-1 day'
      ELSE '>1 day'
    END AS time_band
  FROM question_answer_times
)
SELECT 
  DATE_TRUNC('month', question_date) AS month,
  time_band,
  COUNT(*) AS question_count
FROM time_bands
GROUP BY month, time_band
ORDER BY month, time_band


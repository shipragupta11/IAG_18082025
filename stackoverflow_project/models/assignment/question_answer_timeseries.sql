-- This model creates a time series of questions with accepted answers, banded by the time taken to accept the answer.

{{ config(materialized='table') }}

-- CTE to calculate the time difference (in seconds) between question and accepted answer
WITH question_answer_times AS (
  SELECT 
        q.Id AS QuestionId,                        -- Unique ID for each question
        q.CreationDate AS QuestionCreated,         -- Timestamp when the question was posted
        a.CreationDate AS AcceptedAnswerCreated,   -- Timestamp when the accepted answer was posted
        JULIAN(a.CreationDate) * 86400 - JULIAN(q.CreationDate) * 86400 AS time_to_accept -- Time to accept in seconds

  FROM {{ ref('posts') }} q                        -- Reference to posts table for questions
  JOIN {{ ref('posts') }} a                        -- Reference to posts table for answers
    ON q.AcceptedAnswerId = a.Id                   -- Join questions to their accepted answers
    WHERE q.PostTypeId = 1                         -- Only include questions (PostTypeId = 1)
      AND q.AcceptedAnswerId IS NOT NULL           -- Only include questions with accepted answers
)

-- Aggregate results by month and band the time to accept into defined intervals
SELECT 
    STRFTIME('%Y-%m', sub.QuestionCreated) AS Month, -- Extract year and month from question creation date
    COUNT(CASE WHEN time_to_accept < 60 THEN 1 END) AS "Under 1 min", -- Questions accepted in under 1 minute
    COUNT(CASE WHEN time_to_accept >= 60 AND time_to_accept < 300 THEN 1 END) AS "1-5 mins", -- Accepted in 1-5 minutes
    COUNT(CASE WHEN time_to_accept >= 300 AND time_to_accept < 3600 THEN 1 END) AS "5 mins-1 hour", -- Accepted in 5-60 minutes
    COUNT(CASE WHEN time_to_accept >= 3600 AND time_to_accept < 10800 THEN 1 END) AS "1-3 hours", -- Accepted in 1-3 hours
    COUNT(CASE WHEN time_to_accept >= 10800 AND time_to_accept < 86400 THEN 1 END) AS "3 hours-1 day", -- Accepted in 3-24 hours
    COUNT(CASE WHEN time_to_accept >= 86400 THEN 1 END) AS "Over 1 day" -- Accepted after more than 1 day
FROM question_answer_times sub
GROUP BY month                                      -- Group results by month
ORDER BY month                                      -- Sort results by month
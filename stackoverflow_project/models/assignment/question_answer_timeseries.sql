-- models/question_answer_timeseries.sql
{{ config(materialized='table') }}
WITH question_answer_times AS (
  SELECT 
        q.Id AS QuestionId,
        q.CreationDate AS QuestionCreated,
        a.CreationDate AS AcceptedAnswerCreated,
        JULIAN(a.CreationDate) * 86400 - JULIAN(q.CreationDate) * 86400 AS time_to_accept

  FROM {{ ref('posts') }} q
  JOIN {{ ref('posts') }} a
    ON q.AcceptedAnswerId = a.Id
    WHERE q.PostTypeId = 1 AND q.AcceptedAnswerId IS NOT NULL
)
  SELECT 
    STRFTIME('%Y-%m', sub.QuestionCreated) AS Month,
    COUNT(CASE WHEN time_to_accept < 60 THEN 1 END) AS "Under 1 min",
    COUNT(CASE WHEN time_to_accept >= 60 AND time_to_accept < 300 THEN 1 END) AS "1-5 mins",
    COUNT(CASE WHEN time_to_accept >= 300 AND time_to_accept < 3600 THEN 1 END) AS "5 mins-1 hour",
    COUNT(CASE WHEN time_to_accept >= 3600 AND time_to_accept < 10800 THEN 1 END) AS "1-3 hours",
    COUNT(CASE WHEN time_to_accept >= 10800 AND time_to_accept < 86400 THEN 1 END) AS "3 hours-1 day",
    COUNT(CASE WHEN time_to_accept >= 86400 THEN 1 END) AS "Over 1 day"
  FROM question_answer_times sub
  GROUP BY month
  ORDER BY month

-- models/questions_with_replies.sql
{{ config(materialized='table') }}
WITH question_askers AS (
    SELECT Id AS QuestionId, OwnerUserId
    FROM {{ ref('posts') }} 
    WHERE PostTypeId = 1
),
answers AS (
    SELECT ParentId AS QuestionId, Id AS AnswerId
    FROM {{ ref('posts') }} 
    WHERE PostTypeId = 2
),
all_post_ids AS (
    SELECT QuestionId FROM question_askers
    UNION
    SELECT AnswerId FROM answers
),
comment_mentions AS (
    SELECT c.PostId, c.Text, c.UserId
    FROM {{ ref('comments') }}  AS c
    WHERE c.Text LIKE '@%'
)
SELECT COUNT(DISTINCT q.QuestionId)
FROM question_askers AS q
JOIN all_post_ids AS p ON p.QuestionId = q.QuestionId
JOIN comment_mentions AS cm ON cm.PostId = p.QuestionId AND cm.UserId = q.OwnerUserId


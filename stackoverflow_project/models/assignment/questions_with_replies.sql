-- This model counts how many questions have @replies from the original asker to other users, either on the question or any of its answers.

-- Get all questions and their askers
{{ config(materialized='table') }}

WITH question_askers AS (
    SELECT Id AS QuestionId, OwnerUserId           -- Select question ID and the user who asked it
    FROM {{ ref('posts') }}
    WHERE PostTypeId = 1                           -- Only include questions
),

-- Get all answers and their parent question IDs
answers AS (
    SELECT ParentId AS QuestionId, Id AS AnswerId  -- Select parent question ID and answer ID
    FROM {{ ref('posts') }}
    WHERE PostTypeId = 2                           -- Only include answers
),

-- Collect all post IDs relevant to each question (question itself and its answers)
question_posts AS (
    SELECT QuestionId, QuestionId AS PostId FROM question_askers -- The question itself
    UNION ALL
    SELECT QuestionId, AnswerId AS PostId FROM answers           -- Each answer to the question
),

-- Find questions where the original asker made an @reply on their question or any answer
questions_with_replies AS (
    SELECT DISTINCT qp.QuestionId                   -- Only unique questions
    FROM question_posts qp
    JOIN question_askers qa ON qp.QuestionId = qa.QuestionId     -- Link post to its original asker
    JOIN {{ ref('comments') }} c ON c.PostId = qp.PostId         -- Join comments on question or its answers
    WHERE c.UserId = qa.OwnerUserId                 -- Only comments made by the original asker
      AND c.Text LIKE '%@%'                         -- Only comments containing '@' (an @reply)
)

SELECT COUNT(*) AS questions_with_replies           -- Count the number of such questions
FROM questions_with_replies
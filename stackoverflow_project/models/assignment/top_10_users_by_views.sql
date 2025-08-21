-- This model finds the top 10 users based on the total views of the questions they have posted.

{{ config(materialized='table') }}

WITH user_views AS (
    SELECT 
        p.OwnerUserId AS user_id,         -- Select the user who posted the question
        SUM(p.viewcount) AS total_views   -- Sum up the views for each user's questions
    FROM {{ ref('posts') }} p
    WHERE p.PostTypeId = 1               -- Filter for questions only (PostTypeId = 1)
    GROUP BY p.OwnerUserId               -- Group by user to aggregate views per user
)

SELECT 
    u.id,                                -- User ID
    u.displayname,                       -- User display name
    upv.total_views                      -- Total views on their questions
FROM user_views upv
JOIN {{ ref('users') }} u ON upv.user_id = u.id   -- Join to get user details
ORDER BY upv.total_views DESC            -- Sort by total views, highest first
LIMIT 10                                 -- Return only the top 10 users
-- models/top_10_users_by_views.sql

WITH user_views AS (
    SELECT 
        p.id,
        SUM(p.viewcount) AS total_views
    FROM {{ ref('posts') }} p
    WHERE p.PostTypeId = 'question'
    GROUP BY p.id
)
SELECT 
    u.id, 
    u.displayname, 
    upv.total_views
FROM user_views upv
JOIN {{ ref('users') }} u ON upv.id = u.id
ORDER BY upv.total_views DESC
LIMIT 10

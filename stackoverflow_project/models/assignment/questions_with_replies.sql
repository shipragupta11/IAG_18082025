-- models/questions_with_replies.sql

WITH user_replies AS (
  SELECT 
    c.postid,
    c.userid AS reply_user_id,
    p.OwnerUserId AS question_user_id,
    p.body
  FROM {{ ref('comments') }} c
  JOIN {{ ref('posts') }} p
    ON c.postid = p.id
  WHERE p.posttypeid = 1  -- Only questions
)
SELECT 
  COUNT(DISTINCT ur.postid) AS reply_count
FROM user_replies ur
WHERE ur.reply_user_id != ur.question_user_id  -- Only replies to others, not the original poster
  AND ur.body LIKE '%<>%'  -- <p> indicates a reply

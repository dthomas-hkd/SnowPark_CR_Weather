--CREATE TASK WEATHER_UPDATE_TASK

USE WAREHOUSE COMPUTE_WH;
USE SCHEMA HLB_TEST.PUBLIC;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedur
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK WEATHER_UPDATE_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 MINUTE'
AS
CALL INSERT_INTO_WEATHER();


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

ALTER TASK WEATHER_UPDATE_TASK SUSPEND;
EXECUTE TASK WEATHER_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------


-- Alternatively, here are some manual queries to get at the same details
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-7,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC
;

-- Scheduled task runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;

-- Other task-related metadata queries
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;
---*/
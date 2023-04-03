--CREATE TASK WEATHER_UPDATE_TASK

USE WAREHOUSE WEATHER_WH;
USE SCHEMA CR_WEATHER.WEATHER;
USE ROLE WEATHER_ROLE;

-- ----------------------------------------------------------------------------
-- Step #1: Create the CLEANED_RAW_WEATHER_STREAM to ingest data
-- ----------------------------------------------------------------------------

DESC STREAM RAW_WEATHER_STAGE_STREAM;

SELECT * FROM RAW_WEATHER_STAGE_STREAM;


-- ----------------------------------------------------------------------------
-- Step #2: Create the tasks to call our Python stored procedur
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK WEATHER_UPDATE_TASK
WAREHOUSE = WEATHER_WH
SCHEDULE = '60 MINUTE'
AS
CALL INSERT_INTO_WEATHER();


-- ----------------------------------------------------------------------------
-- Step #3: Execute the tasks
-- ----------------------------------------------------------------------------

ALTER TASK WEATHER_UPDATE_TASK RESUME;
EXECUTE TASK WEATHER_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #4: Monitor tasks in Snowsight
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
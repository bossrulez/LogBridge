--   • Unity Catalog enabled (adjust identifiers as needed).
-- =========================================================
-- LogBridge: Starter Installer (Table + Procedures + View)
-- =========================================================
-- Assumptions:
--   • Unity Catalog enabled (adjust identifiers as needed).
--   • You will pass these job/task params per task:
--       workspace_url = {{workspace.url}}
--       job_id        = {{job.id}}
--       job_run_id    = {{job.run_id}}
--       task_run_id   = {{task.run_id}}
--   • Severity: 1=Error, 2=Info, 3=Debug
-- =========================================================

-- 1) Create the logging table
CREATE TABLE IF NOT EXISTS logbridge_logs (
  WorkspaceUrl STRING,
  JobId        STRING,
  JobRunId     STRING,
  TaskRunId    STRING,
  LogTimeUTC   TIMESTAMP,
  Severity     TINYINT,     -- 1=Error, 2=Info, 3=Debug
  Category     STRING,      -- optional, keep NULL if unused
  MessageId    BIGINT,
  MessageText  STRING,
  DurationMs   INT,
  RowsAffected BIGINT,
  LogTimeCT    TIMESTAMP
)
-- For heavy volumes, uncomment a partitioning strategy you prefer:
-- PARTITIONED BY (date(LogTimeUTC))
;

-- Optional: comment-based docs
COMMENT ON TABLE logbridge_logs IS 'LogBridge centralized app logs with Databricks run context';

-- 2) Core writer procedure
CREATE OR REPLACE PROCEDURE LogBridge_WriteLog (
    WorkspaceUrl STRING,
    JobId        STRING,
    JobRunId     STRING,
    TaskRunId    STRING,
    Severity     TINYINT,        -- 1/2/3
    MessageId    BIGINT,
    MessageText  STRING,
    DurationMs   INT,
    RowsAffected BIGINT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    INSERT INTO logbridge_logs (
        WorkspaceUrl,
        JobId,
        JobRunId,
        TaskRunId,
        LogTimeUTC,
        Severity,
        MessageId,
        MessageText,
        DurationMs,
        RowsAffected
    )
    VALUES (
        WorkspaceUrl,
        JobId,
        JobRunId,
        TaskRunId,
        current_timestamp(),
        Severity,
        MessageId,
        MessageText,
        DurationMs,
        RowsAffected
    );
END;

-- 3) Convenience wrappers (Error / Info / Debug)

-- 3a) Error (severity = 1)
CREATE OR REPLACE PROCEDURE LogBridge_LogError (
    WorkspaceUrl STRING,
    JobId        STRING,
    JobRunId     STRING,
    TaskRunId    STRING,
    MessageId    BIGINT,
    MessageText  STRING,
    DurationMs   INT,
    RowsAffected BIGINT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    CALL LogBridge_WriteLog(
        WorkspaceUrl,
        JobId,
        JobRunId,
        TaskRunId,
        1,  -- Error
        MessageId,
        MessageText,
        DurationMs,
        RowsAffected
    );
END;

-- 3b) Info (severity = 2)
CREATE OR REPLACE PROCEDURE LogBridge_LogInfo (
    WorkspaceUrl STRING,
    JobId        STRING,
    JobRunId     STRING,
    TaskRunId    STRING,
    MessageId    BIGINT,
    MessageText  STRING,
    DurationMs   INT,
    RowsAffected BIGINT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    CALL LogBridge_WriteLog(
        WorkspaceUrl,
        JobId,
        JobRunId,
        TaskRunId,
        2,  -- Info
        MessageId,
        MessageText,
        DurationMs,
        RowsAffected
    );
END;

-- 3c) Debug (severity = 3)
CREATE OR REPLACE PROCEDURE LogBridge_LogDebug (
    WorkspaceUrl STRING,
    JobId        STRING,
    JobRunId     STRING,
    TaskRunId    STRING,
    MessageId    BIGINT,
    MessageText  STRING,
    DurationMs   INT,
    RowsAffected BIGINT
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    CALL LogBridge_WriteLog(
        WorkspaceUrl,
        JobId,
        JobRunId,
        TaskRunId,
        3,  -- Debug
        MessageId,
        MessageText,
        DurationMs,
        RowsAffected
    );
END;

-- 4) Unified view (joins logs with Lakeflow system tables and builds URLs)
CREATE OR REPLACE VIEW logbridge_logs_vw AS
WITH
-- Latest job definition for friendly names (SCD2 collapse)
most_recent_jobs AS (
  SELECT *
  FROM (
    SELECT
      j.*,
      ROW_NUMBER() OVER (
        PARTITION BY j.workspace_id, j.job_id
        ORDER BY j.change_time DESC
      ) AS rn
    FROM system.lakeflow.jobs j
  )
  WHERE rn = 1
),

-- Collapse job run timeline into one row per run
job_runs AS (
  SELECT
    t.workspace_id,
    t.job_id,
    t.run_id,
    MIN(t.period_start_time) AS run_start_time,
    MAX(t.period_end_time)   AS run_end_time,
    MAX_BY(t.result_state,      t.period_end_time) AS job_result_state,
    MAX_BY(t.termination_code,  t.period_end_time) AS job_termination_code,
    MAX_BY(t.trigger_type,      t.period_end_time) AS trigger_type
  FROM system.lakeflow.job_run_timeline t
  GROUP BY t.workspace_id, t.job_id, t.run_id
),

-- Collapse task run timeline into one row per task run
task_runs AS (
  SELECT
    tr.workspace_id,
    tr.job_id,
    tr.job_run_id,
    tr.run_id AS task_run_id,
    tr.task_key,
    MIN(tr.period_start_time) AS task_start_time,
    MAX(tr.period_end_time)   AS task_end_time,
    MAX_BY(tr.result_state,      tr.period_end_time) AS task_result_state,
    MAX_BY(tr.termination_code,  tr.period_end_time) AS task_termination_code
  FROM system.lakeflow.job_task_run_timeline tr
  GROUP BY tr.workspace_id, tr.job_id, tr.job_run_id, tr.run_id, tr.task_key
)

SELECT
  -- Base fields
  l.LogTimeUTC,
  l.WorkspaceUrl,
  l.JobId,
  l.JobRunId,
  l.TaskRunId,
  l.Severity,
  l.Category,
  l.MessageId,
  l.MessageText,
  l.DurationMs,
  l.RowsAffected,

  -- Job enrichment
  mrj.name AS job_name,

  -- Hyperlinks
  CONCAT(REGEXP_REPLACE(l.WorkspaceUrl, '\\?.*$', ''), 'jobs/', l.JobId) AS JobUrl,
  CONCAT(REGEXP_REPLACE(l.WorkspaceUrl, '\\?.*$', ''), 'jobs/', l.JobId, '/runs/', l.JobRunId) AS JobRunUrl
  ,CASE
    WHEN l.TaskRunId IS NOT NULL THEN
      CONCAT(REGEXP_REPLACE(l.WorkspaceUrl, '\\?.*$', ''), 'jobs/', l.JobId, '/runs/', l.TaskRunId)
    ELSE
      CONCAT(REGEXP_REPLACE(l.WorkspaceUrl, '\\?.*$', ''), 'jobs/', l.JobId, '/runs/', l.JobRunId)
  END AS TaskRunUrl

FROM logbridge_logs l
LEFT JOIN job_runs jr_exact
  ON l.JobRunId = jr_exact.run_id
LEFT JOIN job_runs jr_win
  ON l.JobRunId IS NULL
 AND l.JobId = jr_win.job_id
 AND l.LogTimeUTC BETWEEN jr_win.run_start_time AND jr_win.run_end_time
LEFT JOIN most_recent_jobs mrj
  ON COALESCE(jr_exact.workspace_id, jr_win.workspace_id) = mrj.workspace_id
 AND COALESCE(jr_exact.job_id,       jr_win.job_id)       = mrj.job_id
LEFT JOIN task_runs tr_exact
  ON l.TaskRunId = tr_exact.task_run_id
;

-- 5) (Optional) Privileges (adjust to your UC principals)
-- GRANT SELECT ON VIEW  logbridge_logs_vw TO `analyst_group`;
-- GRANT INSERT ON TABLE logbridge_logs TO `platform_service_principal`;
-- GRANT EXECUTE ON PROCEDURE LogBridge_WriteLog  TO `platform_service_principal`;
-- GRANT EXECUTE ON PROCEDURE LogBridge_LogError  TO `platform_service_principal`;
-- GRANT EXECUTE ON PROCEDURE LogBridge_LogInfo   TO `platform_service_principal`;
-- GRANT EXECUTE ON PROCEDURE LogBridge_LogDebug  TO `platform_service_principal`;


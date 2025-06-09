-- Schema for tick data export jobs

-- Create tick_export_jobs table
CREATE TABLE IF NOT EXISTS tick_export_jobs (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE,
    index_name TEXT NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    format TEXT NOT NULL DEFAULT 'parquet',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    tick_count INTEGER,
    file_path TEXT,
    file_size_bytes BIGINT,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP
);

-- Create index on job_id for faster lookups
CREATE INDEX IF NOT EXISTS tick_export_jobs_job_id_idx ON tick_export_jobs (job_id);

-- Create index on status for filtering
CREATE INDEX IF NOT EXISTS tick_export_jobs_status_idx ON tick_export_jobs (status);

-- Create index on index_name for filtering
CREATE INDEX IF NOT EXISTS tick_export_jobs_index_name_idx ON tick_export_jobs (index_name);

-- Create index on created_at for sorting
CREATE INDEX IF NOT EXISTS tick_export_jobs_created_at_idx ON tick_export_jobs (created_at);

-- Comments
COMMENT ON TABLE tick_export_jobs IS 'Stores information about tick data export jobs';
COMMENT ON COLUMN tick_export_jobs.job_id IS 'Unique identifier for the job';
COMMENT ON COLUMN tick_export_jobs.index_name IS 'Name of the index (NIFTY or SENSEX)';
COMMENT ON COLUMN tick_export_jobs.start_date IS 'Start date for data export';
COMMENT ON COLUMN tick_export_jobs.end_date IS 'End date for data export';
COMMENT ON COLUMN tick_export_jobs.format IS 'Export format (parquet or csv)';
COMMENT ON COLUMN tick_export_jobs.status IS 'Job status (pending, running, completed, failed, failed_permanent)';
COMMENT ON COLUMN tick_export_jobs.created_at IS 'Time when the job was created';
COMMENT ON COLUMN tick_export_jobs.started_at IS 'Time when the job started running';
COMMENT ON COLUMN tick_export_jobs.completed_at IS 'Time when the job completed';
COMMENT ON COLUMN tick_export_jobs.tick_count IS 'Number of ticks exported';
COMMENT ON COLUMN tick_export_jobs.file_path IS 'Path to the exported file';
COMMENT ON COLUMN tick_export_jobs.file_size_bytes IS 'Size of the exported file in bytes';
COMMENT ON COLUMN tick_export_jobs.error_message IS 'Error message if the job failed';
COMMENT ON COLUMN tick_export_jobs.retry_count IS 'Number of retry attempts';
COMMENT ON COLUMN tick_export_jobs.next_retry_at IS 'Time for the next retry attempt';

-- Create processing_status table for tracking data processing runs
-- This table helps us know which rows/tables have been processed

CREATE TABLE IF NOT EXISTS public.processing_status (
    status_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    description TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'started', -- 'started', 'processing', 'completed', 'failed'
    records_processed INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    error_message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_processing_status_table_name ON public.processing_status(table_name);
CREATE INDEX IF NOT EXISTS idx_processing_status_status ON public.processing_status(status);
CREATE INDEX IF NOT EXISTS idx_processing_status_created_at ON public.processing_status(created_at);

-- Create a view for easy monitoring
CREATE OR REPLACE VIEW public.processing_status_summary AS
SELECT 
    table_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    SUM(CASE WHEN status = 'started' THEN 1 ELSE 0 END) as running_runs,
    SUM(records_processed) as total_records_processed,
    SUM(records_created) as total_records_created,
    SUM(records_skipped) as total_records_skipped,
    MAX(created_at) as last_run_time,
    MAX(CASE WHEN status = 'completed' THEN end_time END) as last_successful_completion
FROM public.processing_status
GROUP BY table_name
ORDER BY last_run_time DESC;

COMMENT ON TABLE public.processing_status IS 'Tracks processing runs for each table to monitor data import progress';
COMMENT ON COLUMN public.processing_status.table_name IS 'Name of the table being processed (e.g., people, titles, actors)';
COMMENT ON COLUMN public.processing_status.status IS 'Current status: started, processing, completed, failed';
COMMENT ON COLUMN public.processing_status.records_processed IS 'Total number of records processed in this run';
COMMENT ON COLUMN public.processing_status.records_created IS 'Number of new records created in main tables';
COMMENT ON COLUMN public.processing_status.records_skipped IS 'Number of records skipped (duplicates, errors, etc.)';

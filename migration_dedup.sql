ALTER TABLE events
  ADD COLUMN IF NOT EXISTS additional_sources JSONB DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_events_date_start ON events(date_start);

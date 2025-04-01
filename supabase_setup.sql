-- Create user_alerts table
CREATE TABLE IF NOT EXISTS user_alerts (
    user_id BIGINT PRIMARY KEY,
    enabled BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for user_alerts table
CREATE TRIGGER update_user_alerts_updated_at
    BEFORE UPDATE ON user_alerts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Enable RLS
ALTER TABLE user_alerts ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY "Users can read their own alert preferences"
    ON user_alerts FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "Users can update their own alert preferences"
    ON user_alerts FOR UPDATE
    USING (auth.uid() = user_id);

CREATE POLICY "Users can insert their own alert preferences"
    ON user_alerts FOR INSERT
    WITH CHECK (auth.uid() = user_id);

-- Create index
CREATE INDEX idx_user_alerts_user_id ON user_alerts(user_id); 
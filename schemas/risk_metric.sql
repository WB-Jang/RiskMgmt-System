CREATE TABLE risk_daily_metric (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    metric_date DATE NOT NULL,
    metric_code VARCHAR(50) NOT NULL,     -- LCR/NSFR/VAR 등
    metric_value DECIMAL(20, 2) NOT NULL,
    calc_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    remarks VARCHAR(255)
);
CREATE INDEX idx_risk_metric_date ON risk_daily_metric (metric_date);

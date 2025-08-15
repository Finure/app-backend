CREATE TABLE IF NOT EXISTS application_record (
    id BIGINT PRIMARY KEY,
    age INT NOT NULL,
    income INT NOT NULL,
    employed BOOLEAN NOT NULL,
    credit_score INT NOT NULL,
    loan_amount INT NOT NULL,
    approved BOOLEAN NOT NULL,
    pos_prob_initial NUMERIC(5,4),
    pos_prob_latest NUMERIC(5,4),
    risky BOOLEAN,
    risk_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SCHEMA web_banking;
CREATE SCHEMA mobile_banking;


CREATE TABLE web_banking.customers (
    customer_id        BIGSERIAL PRIMARY KEY,
    first_name         VARCHAR(50) NOT NULL,
    last_name          VARCHAR(50) NOT NULL,
    dob                DATE NOT NULL,
    email              VARCHAR(150) UNIQUE NOT NULL,
    phone_number       VARCHAR(20),
    city               VARCHAR(100),
    income_band        VARCHAR(20) CHECK (income_band IN ('LOW','MID','HIGH','PREMIUM')),
    risk_segment       VARCHAR(20) CHECK (risk_segment IN ('LOW','MEDIUM','HIGH')),
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted         BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_web_customers_updated_at 
ON web_banking.customers(updated_at);





CREATE TABLE web_banking.accounts (
    account_id     BIGSERIAL PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    account_type   VARCHAR(20) CHECK (account_type IN ('SAVINGS','CURRENT','CREDIT')),
    open_date      DATE NOT NULL,
    status         VARCHAR(20) CHECK (status IN ('ACTIVE','CLOSED','SUSPENDED')),
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted     BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT fk_web_account_customer
        FOREIGN KEY (customer_id)
        REFERENCES web_banking.customers(customer_id)
);

CREATE INDEX idx_web_accounts_customer 
ON web_banking.accounts(customer_id);

CREATE INDEX idx_web_accounts_updated_at 
ON web_banking.accounts(updated_at);





CREATE TABLE web_banking.transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL,
    txn_timestamp       TIMESTAMP NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    txn_type            VARCHAR(50),
    balance_after_txn   NUMERIC(18,2),
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted          BOOLEAN DEFAULT FALSE,

    CONSTRAINT fk_web_txn_account
        FOREIGN KEY (account_id)
        REFERENCES web_banking.accounts(account_id)
);

CREATE INDEX idx_web_txn_account 
ON web_banking.transactions(account_id);

CREATE INDEX idx_web_txn_timestamp 
ON web_banking.transactions(txn_timestamp);

CREATE INDEX idx_web_txn_updated_at 
ON web_banking.transactions(updated_at);





--==========================================Mobile Schema================

CREATE TABLE mobile_banking.users (
    user_id            BIGSERIAL PRIMARY KEY,
    full_name          VARCHAR(120) NOT NULL,
    birth_date         DATE NOT NULL,
    email_address      VARCHAR(150) UNIQUE NOT NULL,
    mobile_no          VARCHAR(20),
    city_name          VARCHAR(100),
    income_category    VARCHAR(20) CHECK (income_category IN ('L','M','H','P')),
    risk_flag          INTEGER CHECK (risk_flag IN (0,1)),
    created_on         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_on        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_flag        BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_mobile_users_modified_on
ON mobile_banking.users(modified_on);




CREATE TABLE mobile_banking.accounts_mobile (
    acct_id        BIGSERIAL PRIMARY KEY,
    user_id        BIGINT NOT NULL,
    acct_type      VARCHAR(20),
    opened_on      DATE NOT NULL,
    acct_status    VARCHAR(20),
    created_on     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_on    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_flag    BOOLEAN DEFAULT FALSE,

    CONSTRAINT fk_mobile_account_user
        FOREIGN KEY (user_id)
        REFERENCES mobile_banking.users(user_id)
);

CREATE INDEX idx_mobile_accounts_user
ON mobile_banking.accounts_mobile(user_id);

CREATE INDEX idx_mobile_accounts_modified_on
ON mobile_banking.accounts_mobile(modified_on);



CREATE TABLE mobile_banking.txn_mobile (
    transaction_id      BIGSERIAL PRIMARY KEY,
    acct_id             BIGINT NOT NULL,
    txn_time            TIMESTAMP NOT NULL,
    txn_amount          NUMERIC(18,2) NOT NULL,
    transaction_code    VARCHAR(50),
    balance_post_txn    NUMERIC(18,2),
    modified_on         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_flag         BOOLEAN DEFAULT FALSE,

    CONSTRAINT fk_mobile_txn_account
        FOREIGN KEY (acct_id)
        REFERENCES mobile_banking.accounts_mobile(acct_id)
);

CREATE INDEX idx_mobile_txn_acct
ON mobile_banking.txn_mobile(acct_id);

CREATE INDEX idx_mobile_txn_time
ON mobile_banking.txn_mobile(txn_time);

CREATE INDEX idx_mobile_txn_modified
ON mobile_banking.txn_mobile(modified_on);



--=================================Generic function===============

CREATE OR REPLACE FUNCTION web_banking.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER trg_web_customers_updated
BEFORE UPDATE ON web_banking.customers
FOR EACH ROW
EXECUTE FUNCTION web_banking.set_updated_at();


CREATE TRIGGER trg_web_accounts_updated
BEFORE UPDATE ON web_banking.accounts
FOR EACH ROW
EXECUTE FUNCTION web_banking.set_updated_at();


CREATE TRIGGER trg_web_txn_updated
BEFORE UPDATE ON web_banking.transactions
FOR EACH ROW
EXECUTE FUNCTION web_banking.set_updated_at();


---====================================Mobile Function=========

CREATE OR REPLACE FUNCTION mobile_banking.set_modified_on()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_on = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER trg_mobile_users_modified
BEFORE UPDATE ON mobile_banking.users
FOR EACH ROW
EXECUTE FUNCTION mobile_banking.set_modified_on();


CREATE TRIGGER trg_mobile_accounts_modified
BEFORE UPDATE ON mobile_banking.accounts_mobile
FOR EACH ROW
EXECUTE FUNCTION mobile_banking.set_modified_on();

CREATE TRIGGER trg_mobile_txn_modified
BEFORE UPDATE ON mobile_banking.txn_mobile
FOR EACH ROW
EXECUTE FUNCTION mobile_banking.set_modified_on();


SELECT COUNT(*) FROM web_banking.customers;
SELECT * FROM web_banking.customers LIMIT 5;
TRUNCATE web_banking.customers RESTART IDENTITY CASCADE;



TRUNCATE web_banking.accounts RESTART IDENTITY CASCADE;

SELECT COUNT(*) FROM web_banking.accounts;
SELECT account_type, COUNT(*)
FROM web_banking.accounts
GROUP BY account_type;


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'web_banking'
AND table_name = 'accounts';


SELECT COUNT(*) FROM web_banking.accounts;

SELECT status, COUNT(*)
FROM web_banking.accounts
GROUP BY status;


SELECT conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE t.relname = 'accounts'
AND n.nspname = 'web_banking';

SELECT account_type, COUNT(*)
FROM web_banking.accounts
GROUP BY account_type;


SELECT conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE t.relname = 'accounts'
AND n.nspname = 'web_banking'
AND conname LIKE '%status%';



SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'web_banking'
AND table_name = 'transactions';

SELECT conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE t.relname = 'transactions'
AND n.nspname = 'web_banking';

TRUNCATE web_banking.transactions RESTART IDENTITY;


SELECT COUNT(*) FROM web_banking.transactions;
select * from web_banking.accounts limit 10;
select count(*) from web_banking.customers


SELECT txn_type, COUNT(*)
FROM web_banking.transactions
GROUP BY txn_type;




SELECT pg_size_pretty(pg_total_relation_size('web_banking.transactions'));
SELECT pg_size_pretty(pg_total_relation_size('web_banking.customers'));
SELECT pg_size_pretty(pg_total_relation_size('web_banking.accounts'));


SELECT MIN(txn_timestamp), MAX(txn_timestamp)
FROM web_banking.transactions;

SELECT *
FROM web_banking.transactions
ORDER BY txn_timestamp
LIMIT 10;



SELECT COUNT(*) FROM web_banking.customers WHERE is_deleted = true;
SELECT COUNT(*) FROM web_banking.accounts WHERE status = 'CLOSED';-->1289

-- Customers soft deleted
SELECT COUNT(*) 
FROM web_banking.customers 
WHERE is_deleted = true;

-- Updated customers
SELECT COUNT(*) 
FROM web_banking.customers
WHERE updated_at > created_at;

-- Closed accounts
SELECT COUNT(*) 
FROM web_banking.accounts 
WHERE status = 'CLOSED';

SELECT conname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE t.relname = 'customers'
AND n.nspname = 'web_banking'
AND conname LIKE '%income%';

SELECT pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE t.relname = 'customers'
AND n.nspname = 'web_banking'
AND c.conname = 'customers_income_band_check';

select 
--*
--distinct(status)
status,
count(*)
from 
web_banking.accounts 
group by status




-- Customers soft deleted
SELECT COUNT(*) 
FROM web_banking.customers 
WHERE is_deleted = true;

-- Customers updated
SELECT COUNT(*) 
FROM web_banking.customers 
WHERE updated_at > created_at;

-- Accounts closed
SELECT COUNT(*) 
FROM web_banking.accounts 
WHERE status = 'CLOSED';

-- Accounts soft deleted
SELECT COUNT(*) 
FROM web_banking.accounts 
WHERE is_deleted = true;



--Understand Current Activity Distribution
SELECT 
    DATE_TRUNC('month', txn_timestamp) AS month,
    COUNT(*) AS txn_count
FROM web_banking.transactions
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW analytics.customer_churn_view AS
SELECT churn_flag, COUNT(*) 
FROM (
    
WITH last_activity AS (
    SELECT 
        a.customer_id,
        MAX(t.txn_timestamp) AS last_txn
    FROM web_banking.accounts a
    LEFT JOIN web_banking.transactions t
        ON a.account_id = t.account_id
    WHERE a.is_deleted = false
    GROUP BY a.customer_id
),

account_status AS (
    SELECT 
        customer_id,
        COUNT(*) FILTER (WHERE status != 'CLOSED') AS active_accounts
    FROM web_banking.accounts
    WHERE is_deleted = false
    GROUP BY customer_id
)

SELECT 
    c.customer_id,
    l.last_txn,
    a.active_accounts,
    c.is_deleted,

    CASE 
        WHEN c.is_deleted = true THEN 1
        WHEN a.active_accounts = 0 THEN 1
        WHEN l.last_txn IS NULL THEN 1
        WHEN l.last_txn < CURRENT_DATE - INTERVAL '90 days' THEN 1
        ELSE 0
    END AS churn_flag

FROM web_banking.customers c
LEFT JOIN last_activity l 
    ON c.customer_id = l.customer_id
LEFT JOIN account_status a
    ON c.customer_id = a.customer_id
) x
GROUP BY churn_flag;





SELECT 
*
--count(*)
FROM analytics.customer_churn_view;

select * from analytics.churn_rate_view;



CREATE SCHEMA IF NOT EXISTS analytics;


SELECT COUNT(*) FROM web_banking.customers;
SELECT COUNT(*) FROM analytics.customer_churn_view;


drop view analytics.churn_rate_view;
drop view analytics.customer_churn_view;

CREATE OR REPLACE VIEW analytics.customer_churn_view AS

WITH last_activity AS (
    SELECT 
        a.customer_id,
        MAX(t.txn_timestamp) AS last_txn
    FROM web_banking.accounts a
    LEFT JOIN web_banking.transactions t
        ON a.account_id = t.account_id
    GROUP BY a.customer_id
),

account_status AS (
    SELECT 
        customer_id,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_accounts
    FROM web_banking.accounts
    GROUP BY customer_id
)

SELECT 
    c.customer_id,
    l.last_txn,
    COALESCE(a.active_accounts, 0) AS active_accounts,
    c.is_deleted,

    CASE 
        WHEN c.is_deleted = true THEN 1
        WHEN COALESCE(a.active_accounts, 0) = 0 THEN 1
        WHEN l.last_txn IS NULL THEN 1
        WHEN l.last_txn < CURRENT_DATE - INTERVAL '90 days' THEN 1
        ELSE 0
    END AS churn_flag

FROM web_banking.customers c
LEFT JOIN last_activity l 
    ON c.customer_id = l.customer_id
LEFT JOIN account_status a
    ON c.customer_id = a.customer_id;





drop view analytics.customer_churn_view
drop view analytics.churn_rate_view

CREATE OR REPLACE VIEW analytics.customer_churn_view AS
WITH last_activity AS (
    SELECT 
        a.customer_id,
        MAX(t.txn_timestamp) AS last_txn
    FROM web_banking.accounts a
    LEFT JOIN web_banking.transactions t
        ON a.account_id = t.account_id
		AND t.is_deleted = false
    GROUP BY a.customer_id
),

account_status AS (
    SELECT 
        customer_id,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_accounts
    FROM web_banking.accounts
    GROUP BY customer_id
)

SELECT 
    c.customer_id,
    l.last_txn,
    COALESCE(a.active_accounts, 0) AS active_accounts,
    c.is_deleted,

    CASE 
        WHEN c.is_deleted = true THEN 'SOFT_DELETE'
        WHEN COALESCE(a.active_accounts, 0) = 0 THEN 'ALL_ACCOUNTS_CLOSED'
        WHEN l.last_txn IS NULL THEN 'NO_ACTIVITY'
        WHEN l.last_txn < CURRENT_DATE - INTERVAL '90 days' THEN 'INACTIVE_90_DAYS'
        ELSE 'ACTIVE'
    END AS churn_reason,

    CASE 
        WHEN c.is_deleted = true THEN 1
        WHEN COALESCE(a.active_accounts, 0) = 0 THEN 1
        WHEN l.last_txn IS NULL THEN 1
        WHEN l.last_txn < CURRENT_DATE - INTERVAL '90 days' THEN 1
        ELSE 0
    END AS churn_flag

FROM web_banking.customers c
LEFT JOIN last_activity l 
    ON c.customer_id = l.customer_id
LEFT JOIN account_status a
    ON c.customer_id = a.customer_id;




--Churning rate
SELECT churn_reason, COUNT(*)
FROM analytics.customer_churn_view
GROUP BY churn_reason
ORDER BY COUNT(*) DESC;

--Check If Anyone Is Inactive
SELECT COUNT(*)
FROM analytics.customer_churn_view
WHERE churn_reason = 'INACTIVE_90_DAYS';


CREATE OR REPLACE VIEW analytics.churn_rate_view 
AS
--Create Churn Rate View
SELECT 
    COUNT(*) AS total_customers,
    SUM(churn_flag) AS churned_customers,
    ROUND(
        SUM(churn_flag)::numeric / COUNT(*) * 100, 
        2
    ) AS churn_rate_percent
FROM analytics.customer_churn_view;

select * from 
analytics.churn_rate_view





--Build Monthly Aggregation View
CREATE OR REPLACE VIEW analytics.customer_monthly_activity AS
SELECT
    a.customer_id,
    DATE_TRUNC('month', t.txn_timestamp)::date AS snapshot_month,

    COUNT(*) AS txn_count,
    SUM(t.amount) AS total_amount,
    SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow_amount,
    SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) AS outflow_amount,
    AVG(t.amount) AS avg_txn_amount

FROM web_banking.transactions t
JOIN web_banking.accounts a
    ON t.account_id = a.account_id

WHERE t.is_deleted = false

GROUP BY
    a.customer_id,
    DATE_TRUNC('month', t.txn_timestamp);

select * from analytics.customer_monthly_activity





--full snapshot view
--drop view analytics.customer_monthly_snapshot
CREATE OR REPLACE VIEW analytics.customer_monthly_snapshot AS

WITH monthly_activity AS (
    SELECT *
    FROM analytics.customer_monthly_activity
),

future_activity AS (
    SELECT
        m.customer_id,
        m.snapshot_month,

        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM web_banking.transactions t
                JOIN web_banking.accounts a
                    ON t.account_id = a.account_id
                WHERE a.customer_id = m.customer_id
                AND t.is_deleted = false
                AND t.txn_timestamp > m.snapshot_month
                AND t.txn_timestamp <= m.snapshot_month + INTERVAL '90 days'
            )
            THEN 1
            ELSE 0
        END AS churn_next_90_flag

    FROM monthly_activity m
)

SELECT
    m.*,
    f.churn_next_90_flag

FROM monthly_activity m
LEFT JOIN future_activity f
    ON m.customer_id = f.customer_id
    AND m.snapshot_month = f.snapshot_month;


select count(*) from analytics.customer_monthly_snapshot
--select * from analytics.customer_monthly_snapshot limit 10;


SELECT 
    churn_next_90_flag, 
    COUNT(*)
FROM analytics.customer_monthly_snapshot
GROUP BY churn_next_90_flag;



---Compute Churn Event Date

CREATE OR REPLACE VIEW analytics.customer_churn_event AS

WITH last_txn AS (
    SELECT
        a.customer_id,
        MAX(t.txn_timestamp)::date AS last_txn_date
    FROM web_banking.transactions t
    JOIN web_banking.accounts a
        ON t.account_id = a.account_id
    WHERE t.is_deleted = false
    GROUP BY a.customer_id
),

account_closed AS (
    SELECT
        customer_id,
        MAX(updated_at)::date AS account_closed_date
    FROM web_banking.accounts
    WHERE status = 'CLOSED'
    GROUP BY customer_id
),

customer_deleted AS (
    SELECT
        customer_id,
        updated_at::date AS deleted_date
    FROM web_banking.customers
    WHERE is_deleted = true
)

SELECT
    c.customer_id,

    LEAST(
        COALESCE(ac.account_closed_date, '9999-12-31'),
        COALESCE(cd.deleted_date, '9999-12-31'),
        COALESCE(lt.last_txn_date + INTERVAL '90 days', '9999-12-31')
    ) AS churn_event_date

FROM web_banking.customers c
LEFT JOIN last_txn lt ON c.customer_id = lt.customer_id
LEFT JOIN account_closed ac ON c.customer_id = ac.customer_id
LEFT JOIN customer_deleted cd ON c.customer_id = cd.customer_id;



--rebuild snapshot with correct label

CREATE OR REPLACE VIEW analytics.customer_monthly_snapshot AS

WITH monthly_activity AS (
    SELECT *
    FROM analytics.customer_monthly_activity
),

churn_event AS (
    SELECT *
    FROM analytics.customer_churn_event
)

SELECT
    m.*,

    CASE
        WHEN ce.churn_event_date <= m.snapshot_month + INTERVAL '90 days'
         AND ce.churn_event_date > m.snapshot_month
        THEN 1
        ELSE 0
    END AS churn_next_90_flag

FROM monthly_activity m
LEFT JOIN churn_event ce
    ON m.customer_id = ce.customer_id;




SELECT churn_next_90_flag, COUNT(*)
FROM analytics.customer_monthly_snapshot
GROUP BY churn_next_90_flag;




DROP VIEW analytics.customer_monthly_snapshot;
/* PHASE A — Advanced Feature Engineering
	We will add rolling behavioral features
		For each (customer_id, snapshot_month):
			A. Rolling 3-Month Metrics
				1)rolling_3m_txn_count
				2)rolling_3m_outflow
				3)rolling_3m_inflow
			B. Rolling 6-Month Metrics
				1)rolling_6m_txn_count
				2)rolling_6m_outflow
			C.  Trend Features
				1)txn_count_change_vs_prev_month
				2)outflow_change_vs_prev_month
			D.Tenure Feature
				1)months_since_first_txn
These make your dataset ML-grade.
*/

CREATE OR REPLACE VIEW analytics.customer_monthly_snapshot AS

WITH base AS (
    SELECT *
    FROM analytics.customer_monthly_activity
),

churn_event AS (
    SELECT *
    FROM analytics.customer_churn_event
),

enhanced AS (
    SELECT
        b.*,

        -- Rolling 3 month metrics
        SUM(txn_count) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_txn_count,

        SUM(outflow_amount) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_outflow,

        SUM(inflow_amount) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_inflow,

        -- Rolling 6 month metrics
        SUM(txn_count) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_6m_txn_count,

        SUM(outflow_amount) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_6m_outflow,

        -- Month over month delta
        txn_count - LAG(txn_count) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
        ) AS txn_count_mom_change,

        outflow_amount - LAG(outflow_amount) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_month
        ) AS outflow_mom_change,

        -- Tenure
        EXTRACT(
            MONTH FROM AGE(snapshot_month, MIN(snapshot_month) OVER (PARTITION BY customer_id))
        ) AS months_since_first_txn

    FROM base b
)

SELECT
    e.*,

    CASE
        WHEN ce.churn_event_date <= e.snapshot_month + INTERVAL '90 days'
         AND ce.churn_event_date > e.snapshot_month
        THEN 1
        ELSE 0
    END AS churn_next_90_flag

FROM enhanced e
LEFT JOIN churn_event ce
    ON e.customer_id = ce.customer_id;




SELECT * 
FROM analytics.customer_monthly_snapshot
ORDER BY snapshot_month DESC
LIMIT 5;

--Monthly Churn Rate Trend
CREATE OR REPLACE VIEW analytics.monthly_churn_trend AS
SELECT
    snapshot_month,
    COUNT(*) AS total_customers,
    SUM(churn_next_90_flag) AS churned_customers,
    ROUND(
        SUM(churn_next_90_flag)::numeric / COUNT(*) * 100,
        2
    ) AS churn_rate_percent
FROM analytics.customer_monthly_snapshot
GROUP BY snapshot_month
ORDER BY snapshot_month;

SELECT * FROM analytics.monthly_churn_trend;



--Cohort Analysis (Customer Age Buckets vs Churn)
CREATE OR REPLACE VIEW analytics.customer_cohort_analysis AS

WITH base AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', c.created_at)::date AS cohort_month,
        s.snapshot_month,
        s.churn_next_90_flag
    FROM web_banking.customers c
    JOIN analytics.customer_monthly_snapshot s
        ON c.customer_id = s.customer_id
)

SELECT
    cohort_month,
    snapshot_month,
    COUNT(*) AS total_customers,
    SUM(churn_next_90_flag) AS churned_customers,
    ROUND(
        SUM(churn_next_90_flag)::numeric / COUNT(*) * 100,
        2
    ) AS churn_rate_percent
FROM base
GROUP BY cohort_month, snapshot_month
ORDER BY cohort_month, snapshot_month;



SELECT * 
FROM analytics.customer_cohort_analysis
LIMIT 50;


--Segmentation Trends (Income + Risk)
CREATE OR REPLACE VIEW analytics.churn_by_income_band AS

SELECT
    c.income_band,
    COUNT(*) AS total_customers,
    SUM(s.churn_next_90_flag) AS churned,
    ROUND(
        SUM(s.churn_next_90_flag)::numeric / COUNT(*) * 100,
        2
    ) AS churn_rate_percent
FROM analytics.customer_monthly_snapshot s
JOIN web_banking.customers c
    ON s.customer_id = c.customer_id
GROUP BY c.income_band
ORDER BY churn_rate_percent DESC;




--Risk Segment Segmentation
CREATE OR REPLACE VIEW analytics.churn_by_risk_segment AS
SELECT
    c.risk_segment,
    COUNT(*) AS total_customers,
    SUM(s.churn_next_90_flag) AS churned,
    ROUND(
        SUM(s.churn_next_90_flag)::numeric / COUNT(*) * 100,
        2
    ) AS churn_rate_percent
FROM analytics.customer_monthly_snapshot s
JOIN web_banking.customers c
    ON s.customer_id = c.customer_id
GROUP BY c.risk_segment
ORDER BY churn_rate_percent DESC;



SELECT * FROM analytics.churn_by_income_band;
SELECT * FROM analytics.churn_by_risk_segment;




/*  Export to Python for ML
	Now we export the modeling dataset.
	We export only the latest 12 months to keep dataset realistic
	Create ML Dataset View
*/


CREATE OR REPLACE VIEW analytics.ml_training_dataset AS

SELECT *
FROM analytics.customer_monthly_snapshot
WHERE snapshot_month >= (
    SELECT MAX(snapshot_month) - INTERVAL '12 months'
    FROM analytics.customer_monthly_snapshot
);

SELECT COUNT(*) FROM analytics.ml_training_dataset;


CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.dim_date AS
SELECT DISTINCT
    txn_timestamp::date AS date_key,
    EXTRACT(YEAR FROM txn_timestamp) AS year,
    EXTRACT(MONTH FROM txn_timestamp) AS month,
    EXTRACT(DAY FROM txn_timestamp) AS day,
    TO_CHAR(txn_timestamp, 'Month') AS month_name,
    TO_CHAR(txn_timestamp, 'Day') AS day_name,
    EXTRACT(QUARTER FROM txn_timestamp) AS quarter
FROM web_banking.transactions;



CREATE TABLE dw.dim_customer AS
SELECT
    customer_id AS customer_key,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    created_at,
    is_deleted
FROM web_banking.customers;


CREATE TABLE dw.dim_account AS
SELECT
    account_id AS account_key,
    customer_id AS customer_key,
    account_type,
    status,
    open_date,
    is_deleted
FROM web_banking.accounts;

CREATE TABLE dw.fact_transactions AS
SELECT
    t.txn_id,
    t.account_id AS account_key,
    a.customer_id AS customer_key,
    t.txn_timestamp::date AS date_key,
    t.amount,
    t.balance_after_txn
FROM web_banking.transactions t
JOIN web_banking.accounts a
    ON t.account_id = a.account_id
WHERE t.is_deleted = false;


CREATE TABLE dw.fact_customer_monthly AS
SELECT
    customer_id AS customer_key,
    snapshot_month AS month_key,
    txn_count,
    inflow_amount,
    outflow_amount,
    rolling_3m_txn_count,
    rolling_3m_outflow,
    churn_next_90_flag
FROM analytics.customer_monthly_snapshot;


ALTER TABLE dw.fact_transactions
ADD CONSTRAINT fk_fact_customer
FOREIGN KEY (customer_key)
REFERENCES dw.dim_customer(customer_key);

ALTER TABLE dw.fact_transactions
ADD CONSTRAINT fk_fact_account
FOREIGN KEY (account_key)
REFERENCES dw.dim_account(account_key);



SELECT
    d.year,
    d.month,
    SUM(f.amount) AS total_revenue
FROM dw.fact_transactions f
JOIN dw.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;




CREATE TABLE dw.dim_customer_scd (
    surrogate_key SERIAL PRIMARY KEY,
    customer_key BIGINT,

    first_name VARCHAR(100),
    last_name VARCHAR(100),
    income_band VARCHAR(20),
    risk_segment VARCHAR(20),
    city VARCHAR(100),

    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


select * from dw.dim_customer_scd



INSERT INTO dw.dim_customer_scd (
    customer_key,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    customer_id,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM web_banking.customers;


--Simulate Customer Change
UPDATE web_banking.customers
SET income_band = 'HIGH'
WHERE customer_id IN (
    SELECT customer_id
    FROM web_banking.customers
    ORDER BY random()
    LIMIT 50
);




--SCD2 Merge Logic
-- 1️⃣ Expire old versions
UPDATE dw.dim_customer_scd d
SET
    effective_end_date = CURRENT_DATE - 1,
    is_current = FALSE
FROM web_banking.customers s
WHERE d.customer_key = s.customer_id
AND d.is_current = TRUE
AND (
    d.income_band <> s.income_band
    OR d.risk_segment <> s.risk_segment
    OR d.city <> s.city
);

-- 2️⃣ Insert new versions
INSERT INTO dw.dim_customer_scd (
    customer_key,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.income_band,
    s.risk_segment,
    s.city,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM web_banking.customers s
LEFT JOIN dw.dim_customer_scd d
    ON s.customer_id = d.customer_key
    AND d.is_current = TRUE
WHERE
    d.customer_key IS NULL
    OR (
        d.income_band <> s.income_band
        OR d.risk_segment <> s.risk_segment
        OR d.city <> s.city
    );



SELECT customer_key, COUNT(*)
FROM dw.dim_customer_scd
GROUP BY customer_key
HAVING COUNT(*) > 1;



ALTER TABLE dw.fact_transactions
ADD COLUMN customer_skey INT;



UPDATE dw.fact_transactions f
SET customer_skey = d.surrogate_key
FROM dw.dim_customer_scd d
WHERE f.customer_key = d.customer_key
AND f.date_key BETWEEN d.effective_start_date
                   AND d.effective_end_date;



ALTER TABLE dw.fact_transactions
ADD CONSTRAINT fk_fact_customer_scd
FOREIGN KEY (customer_skey)
REFERENCES dw.dim_customer_scd(surrogate_key);



SELECT COUNT(*)
FROM dw.fact_transactions
WHERE customer_skey IS NULL;


DROP TABLE dw.dim_customer_scd;

ALTER TABLE dw.fact_transactions
DROP CONSTRAINT fk_fact_customer_scd;

UPDATE dw.fact_transactions
SET customer_skey = NULL;



CREATE TABLE dw.dim_customer_scd (
    surrogate_key SERIAL PRIMARY KEY,
    customer_key BIGINT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    income_band VARCHAR(20),
    risk_segment VARCHAR(20),
    city VARCHAR(100),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dw.dim_customer_scd (
    customer_key,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    customer_id,
    first_name,
    last_name,
    income_band,
    risk_segment,
    city,
    created_at::date,      -- important fix
    '9999-12-31',
    TRUE
FROM web_banking.customers;

UPDATE dw.fact_transactions
SET customer_skey = NULL;

UPDATE dw.fact_transactions f
SET customer_skey = d.surrogate_key
FROM dw.dim_customer_scd d
WHERE f.customer_key = d.customer_key
AND f.date_key BETWEEN d.effective_start_date
                   AND d.effective_end_date;

ALTER TABLE dw.fact_transactions
ADD CONSTRAINT fk_fact_customer_scd
FOREIGN KEY (customer_skey)
REFERENCES dw.dim_customer_scd(surrogate_key);


SELECT COUNT(*)
FROM dw.fact_transactions
WHERE customer_skey IS NULL;


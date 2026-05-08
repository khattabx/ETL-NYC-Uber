-- ══════════════════════════════════════════════════════════
-- Uber DWH — Star Schema DDL
-- Database : UBER_DWH
-- Schema   : PUBLIC
-- ══════════════════════════════════════════════════════════

-- ── Setup ─────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS UBER_DWH;
USE DATABASE UBER_DWH;
USE SCHEMA PUBLIC;


-- ══════════════════════════════════════════════════════════
-- DIMENSIONS
-- ══════════════════════════════════════════════════════════

-- DIM_DATETIME
CREATE OR REPLACE TABLE DIM_DATETIME (
    datetime_id       NUMBER        NOT NULL PRIMARY KEY,
    pickup_datetime   TIMESTAMP_NTZ NOT NULL,
    dropoff_datetime  TIMESTAMP_NTZ NOT NULL,
    hour              TINYINT,
    day               SMALLINT,
    month             SMALLINT,
    year              SMALLINT,
    weekday           VARCHAR(10),
    trip_duration_min INTEGER
);

-- DIM_LOCATION
CREATE OR REPLACE TABLE DIM_LOCATION (
    location_id   INTEGER      NOT NULL PRIMARY KEY,
    borough       VARCHAR(50),
    zone          VARCHAR(100),
    service_zone  VARCHAR(50)
);

-- DIM_VENDOR
CREATE OR REPLACE TABLE DIM_VENDOR (
    vendor_id      INTEGER     NOT NULL PRIMARY KEY,
    VendorID       TINYINT,
    vendor_name    VARCHAR(50),
    store_fwd_flag CHAR(1)
);

-- DIM_PAYMENT
CREATE OR REPLACE TABLE DIM_PAYMENT (
    payment_id           INTEGER       NOT NULL PRIMARY KEY,
    payment_type         TINYINT,
    payment_desc         VARCHAR(20),
    congestion_surcharge DECIMAL(10,2),
    cbd_congestion_fee   DECIMAL(10,2)
);

-- DIM_RATE
CREATE OR REPLACE TABLE DIM_RATE (
    rate_id      INTEGER      NOT NULL PRIMARY KEY,
    RatecodeID   TINYINT,
    rate_desc    VARCHAR(30),
    airport_fee  DECIMAL(10,2)
);


-- ══════════════════════════════════════════════════════════
-- FACT
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE FACT_TRIP (
    trip_id               NUMBER        NOT NULL PRIMARY KEY,
    datetime_id           NUMBER,
    location_id           INTEGER,
    vendor_id             INTEGER,
    payment_id            INTEGER,
    rate_id               INTEGER,
    passenger_count       INTEGER,
    trip_distance         FLOAT,
    fare_amount           DECIMAL(10,2),
    extra                 DECIMAL(10,2),
    mta_tax               DECIMAL(10,2),
    tip_amount            DECIMAL(10,2),
    tolls_amount          DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount          DECIMAL(10,2),

    -- Foreign Keys
    FOREIGN KEY (datetime_id) REFERENCES DIM_DATETIME(datetime_id),
    FOREIGN KEY (location_id) REFERENCES DIM_LOCATION(location_id),
    FOREIGN KEY (vendor_id)   REFERENCES DIM_VENDOR(vendor_id),
    FOREIGN KEY (payment_id)  REFERENCES DIM_PAYMENT(payment_id),
    FOREIGN KEY (rate_id)     REFERENCES DIM_RATE(rate_id)
);
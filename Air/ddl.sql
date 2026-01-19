CREATE OR REPLACE TABLE AIR_TEST.SRC_AIRLINE_DATASET (
    id                    NUMBER(38,0)
                          AUTOINCREMENT
                          START 1
                          INCREMENT 1
                          NOT NULL,

    passenger_id          STRING,
    first_name            STRING,
    last_name             STRING,
    gender                STRING,
    age                   NUMBER(38,0),
    nationality           STRING,
    airport_name          STRING,
    airport_country_code  STRING,              -- US, CA...
    country_name          STRING,
    airport_continent     STRING,              -- NAM
    continents            STRING,               -- North America
    departure_date        DATE,                 -- из '6/28/2022'
    arrival_airport       STRING,               -- CXF, YCO...
    pilot_name            STRING,
    flight_status         STRING,               -- On Time, Delayed, etc.
    ticket_type           STRING,               -- Business, Economy...
    passenger_status      STRING,               -- On Time, etc.

    update_ts             TIMESTAMP_LTZ
);

-- Документационная PK (не enforced)
ALTER TABLE AIR_TEST.SRC_AIRLINE_DATASET
  ADD CONSTRAINT PK_SRC_AIRLINE_DATASET PRIMARY KEY (id);


CREATE OR REPLACE TABLE AIR_TEST.DIM_CUSTOMER (
    customer_sk        NUMBER AUTOINCREMENT START 1000 INCREMENT 1,

    passenger_id       STRING NOT NULL,          -- business key
    first_name         STRING,
    last_name          STRING,
    gender             STRING,
    age                NUMBER,
    nationality        STRING,

    record_source      STRING DEFAULT 'AIRLINE',
    effective_from_ts  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to_ts    TIMESTAMP_LTZ,
    is_current         BOOLEAN DEFAULT TRUE,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
);


CREATE OR REPLACE TABLE AIR_TEST.DIM_AIRPORT (
    airport_sk             NUMBER AUTOINCREMENT START 10000 INCREMENT 1,

    airport_name           STRING NOT NULL,
    airport_country_code   STRING,
    country_name           STRING,
    airport_continent      STRING,
    continents             STRING,

    record_source          STRING DEFAULT 'AIRLINE',
    effective_from_ts      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to_ts        TIMESTAMP_LTZ,
    is_current             BOOLEAN DEFAULT TRUE,

    CONSTRAINT pk_dim_airport PRIMARY KEY (airport_sk)
);


       CREATE OR REPLACE TABLE AIR_TEST.FACT_FLIGHT (
    customer_sk        NUMBER NOT NULL,
    airport_sk         NUMBER NOT NULL,

    departure_date     DATE,
    arrival_airport    STRING,
    pilot_name         STRING,
    flight_status      STRING,
    ticket_type        STRING,
    passenger_status  STRING,

    update_ts          TIMESTAMP_LTZ,

    record_source      STRING DEFAULT 'AIRLINE',

    CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_sk)
        REFERENCES AIR_TEST.DIM_CUSTOMER(customer_sk),

    CONSTRAINT fk_fact_airport
        FOREIGN KEY (airport_sk)
        REFERENCES AIR_TEST.DIM_AIRPORT(airport_sk)
);

CREATE OR REPLACE TABLE AIR_TEST.RLS_USER_CONTINENT (
    user_name STRING,
    continent STRING
);

INSERT INTO AIR_TEST.RLS_USER_CONTINENT (user_name, continent)
VALUES
    ('user_oc',  'Oceania'),
    ('user_sam', 'South America'),
    ('user_as',  'Asia'),
    ('user_af',  'Africa'),
    ('user_eu',  'Europe'),
    ('user_nam', 'North America');


CREATE OR REPLACE USER user_oc  PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;
CREATE OR REPLACE USER user_sam PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;
CREATE OR REPLACE USER user_as  PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;
CREATE OR REPLACE USER user_af  PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;
CREATE OR REPLACE USER user_eu  PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;
CREATE OR REPLACE USER user_nam PASSWORD='TempPass123!' DEFAULT_ROLE=PUBLIC;

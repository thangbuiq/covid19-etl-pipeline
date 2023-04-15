
DROP TABLE IF EXISTS covid19_cases_position;
CREATE TABLE covid19_cases_position (
    province_state varchar(64),
    `country_region` varchar(64),
    `lat` float4,
    `long` float4,
    `date` varchar(32),
    `confirmed` int4,
    deaths int4,
    recovered int4,
    active int4,
    who_region varchar(64),
    PRIMARY KEY (`lat`,`long`)
);

DROP TABLE IF EXISTS covid19_country_wise;
CREATE TABLE covid19_country_wise (
    country_region varchar(64),
    confirmed int4,
    deaths int4,
    recovered int4,
    active int4,
    new_cases int4,
    new_deaths int4,
    new_recovered int4,
    deaths___100_cases float4,
    recovered___100_cases float4,
    deaths___100_recovered float4,
    confirmed_last_week int4,
    1_week_change int4,
    1_week_rate_increase float4, 
    who_region varchar(64),
    PRIMARY KEY (country_region)
);


DROP TABLE IF EXISTS covid19_time_series;
CREATE TABLE covid19_time_series (
    `date`           date,
    country_region   varchar(64),        
    confirmed        int4,       
    deaths           int4,         
    recovered        int4,         
    active           int4,         
    new_cases        int4,         
    new_deaths       int4,         
    new_recovered    int4,         
    who_region       varchar(64),
    CONSTRAINT PK_covid19_timeseries PRIMARY KEY (`date`, `country_region`)
);

DROP TABLE IF EXISTS covid19_worldometer;
CREATE TABLE covid19_worldometer (
    country_region varchar(64),
    continent varchar(64),
    `population` float4,
    totalcases float4,
    newcases float4,
    totaldeaths float4,
    newdeaths float4,
    totalrecovered float4,
    newrecovered float4,
    activecases float4,
    `serious,critical` float4,
    tot_cases_1m_pop float4,
    deaths_1m_pop float4,
    totaltests float4,
    tests_1m_pop float4,
    who_region varchar(64),
    CONSTRAINT PK_covid19_worldometer PRIMARY KEY (country_region)
);

LOAD DATA LOCAL INFILE '/tmp/covid19-clean/covid19_cases_position.csv' 
INTO TABLE covid19_cases_position FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/covid19-clean/covid19_country_wise.csv' 
INTO TABLE covid19_country_wise FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/covid19-clean/covid19_time_series.csv' 
INTO TABLE covid19_time_series FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/covid19-clean/covid19_worldometer.csv' 
INTO TABLE covid19_worldometer FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

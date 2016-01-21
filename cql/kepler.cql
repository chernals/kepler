--ALTER KEYSPACE system_auth WITH REPLICATION =
--  {'class' : 'NetworkTopologyStrategy', 'CCR': '3'};

CREATE KEYSPACE kepler 
	WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
	AND durable_writes = true;

-- Composed type for Dataset(name, tag)
CREATE TYPE kepler.dataset (
	name              TEXT,
	tag               TEXT
);
	
-- Composed type for parameters (device/property#field)
CREATE TYPE kepler.parameter (
	device            TEXT,
	property          TEXT,
	field             TEXT
);

-- Composed type for 'cycles' composing 'beams'
CREATE TYPE kepler.cycle (
	machine           TEXT,
	injection         INT,
	cyclestamp        TIMESTAMP
);
	
-- Metadata for datasets
CREATE TABLE kepler.md_info(
	name              TEXT,
	tag               TEXT,
	beamstamp         TIMESTAMP,
	cycles            SET<FROZEN<cycle>>, -- Cyclestamps of the cycles composing the beam
	comment           TEXT,
	md_comment        TEXT STATIC,
	users             SET<TEXT> STATIC,
	created           TIMESTAMP STATIC,
	PRIMARY KEY ((name), tag, beamstamp)
) WITH CLUSTERING ORDER BY (tag ASC, beamstamp DESC);

-- Payload data for MD's
CREATE TABLE kepler.md_data(
	dataset           FROZEN<dataset>,
    beamstamp         TIMESTAMP,
	telegram          MAP<FROZEN<cycle>,FROZEN<MAP<TEXT,TEXT>>> STATIC,
	cycle             FROZEN<cycle>,
	parameter         FROZEN<parameter>,
	blob_value        BLOB,
	real_value        FLOAT,
	text_value        TEXT,
	type              TEXT,
   	PRIMARY KEY ((dataset, beamstamp), cycle, parameter)
) WITH CLUSTERING ORDER BY (cycle ASC, parameter ASC);

-- Allow querying by parameter
-- These are 'lightweight' timeseries
CREATE INDEX parameter_idx ON kepler.md_data(parameter);

-- Timeseries
CREATE TABLE kepler.timeseries(
	id                UUID,
	type              TEXT STATIC,
	timestamp         TIMESTAMP,
	blob_value        BLOB,
	real_value        FLOAT,
	text_value        TEXT,
	telegram          MAP<TEXT, TEXT>,
	PRIMARY KEY (id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Description for the device defined timeseries
CREATE TABLE kepler.device_timeseries(
	parameter         FROZEN <parameter>,
	year              INT,
	selector          TEXT,
	description       TEXT,
	id                UUID,
	PRIMARY KEY ((parameter, year), selector)
);

-- Description for the user defined timeseries
CREATE TABLE kepler.user_timeseries(
	name              TEXT,
	tag               TEXT,
	description       TEXT,
	user_names        SET<TEXT>,
	md_names          SET<TEXT>,
	id                UUID,
	PRIMARY KEY (name, tag)
);

-- Analytics on the kepler
--CREATE TABLE kepler.analytics(
	
	
	--);
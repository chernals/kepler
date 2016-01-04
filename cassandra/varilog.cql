CREATE KEYSPACE varilog 
	WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
	AND durable_writes = true;
	
-- Metadata for MD's and tags
CREATE TABLE varilog.md_info(
	name              TEXT,
	tag               TEXT,
	comment           TEXT,
	id                TIMEUUID,
	cyclestamp        TIMESTAMP,
	md_comment        TEXT STATIC,
	users             SET<TEXT> STATIC,
	created           TIMESTAMP STATIC,
	PRIMARY KEY ((name), tag, cyclestamp)
) WITH CLUSTERING ORDER BY (tag ASC, id DESC);

-- Composed type for parameters
-- device/property#field
CREATE TYPE varilog.parameter (
	device            TEXT,
	property          TEXT,
	field             TEXT
);

-- Payload data for MD's
-- Cycle and telegram infos should go in a special 'MTG' device
CREATE TABLE varilog.md_data(
    id                TIMEUUID,
	cyclestamp        TIMESTAMP STATIC,
	parameter         FROZEN <parameter>,
	blob_value        BLOB,
	real_value        FLOAT,
	text_value        TEXT,
	type              TEXT,
   	PRIMARY KEY ((id), parameter)
) WITH CLUSTERING ORDER BY (parameter ASC);

-- Allow querying by parameter
-- These are 'lightweight' timeseries
CREATE INDEX parameter_idx ON varilog.md_data(parameter);

-- Maintain cycles count for each MD 
CREATE TABLE varilog.md_cycles_count(
	name              TEXT,
	tag               TEXT,
	cycles_count      COUNTER,
	PRIMARY KEY ((name), tag)
);

-- Timeseries
CREATE TABLE varilog.timeseries(
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
CREATE TABLE varilog.device_timeseries(
	device            TEXT,
	property          TEXT,
	field             TEXT,
	year              INT,
	selector          TEXT,
	description       TEXT,
	id                UUID,
	PRIMARY KEY ((device, property, field, year), selector)
);

-- Description for the user defined timeseries
CREATE TABLE varilog.user_timeseries(
	name              TEXT,
	tag               TEXT,
	description       TEXT,
	user_names        SET<TEXT>,
	md_names          SET<TEXT>,
	id                UUID,
	PRIMARY KEY (name, tag)
);
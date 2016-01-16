CREATE KEYSPACE varilog 
	WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
	AND durable_writes = true;
	
-- Composed type for parameters
-- device/property#field
CREATE TYPE varilog.parameter (
	device            TEXT,
	property          TEXT,
	field             TEXT
);
	
-- Metadata for MD's and tags
CREATE TABLE varilog.md_info(
	name              TEXT,
	tag               TEXT,
	id                TIMEUUID,
	comment           TEXT,
	cyclestamp        TIMESTAMP,
	md_comment        TEXT STATIC,
	users             SET<TEXT> STATIC,
	created           TIMESTAMP STATIC,
	PRIMARY KEY ((name), tag, id)
) WITH CLUSTERING ORDER BY (tag ASC, id ASC);

-- Allow querying by cyclestamps
CREATE INDEX cyclestamp_idx ON varilog.md_info(cyclestamp);

-- Payload data for MD's
CREATE TABLE varilog.md_data(
	name              TEXT,
	tag               TEXT,
    id                TIMEUUID,
	telegram          MAP<TEXT,TEXT> STATIC,
	parameter         FROZEN <parameter>,
	blob_value        BLOB,
	real_value        FLOAT,
	text_value        TEXT,
	type              TEXT,
   	PRIMARY KEY ((name, tag, id), parameter)
) WITH CLUSTERING ORDER BY (parameter ASC);

-- Allow querying by parameter
-- These are 'lightweight' timeseries
CREATE INDEX parameter_idx ON varilog.md_data(parameter);

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
	parameter         FROZEN <parameter>,
	year              INT,
	selector          TEXT,
	description       TEXT,
	id                UUID,
	PRIMARY KEY ((parameter, year), selector)
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

-- Analytics on the varilog
CREATE TABLE varilog.analytics(
	
	
);
CREATE KEYSPACE varilog 
	WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
	AND durable_writes = true;

-- Table storing the payload data for MD's
-- Cycle infos should go in a special 'MTG' device
CREATE TABLE varilog.md_data(
	name              TEXT,
	tag               TEXT,
	comment           TEXT STATIC,
	users             SET<TEXT> STATIC,
	created_at        TIMESTAMP STATIC,
	cyclestamp        TIMESTAMP,
	device            TEXT,
	property          TEXT,
	field             TEXT,
	blob_value        BLOB,
	real_value        FLOAT,
	text_value        TEXT,
	type              TEXT,
   	PRIMARY KEY ((name, tag), cyclestamp, device, property, field)
) WITH CLUSTERING ORDER BY (cyclestamp DESC, device ASC, property ASC, field ASC);

-- Allow querying by device/property#field
-- These are 'lightweight' timeseries
CREATE INDEX device_idx ON md_data(device);
CREATE INDEX property_idx ON md_data(property);
CREATE INDEX field_idx ON md_data(field);

-- Maintain cycles count for each MD 
CREATE TABLE varilog.md_cycles_count(
	name              TEXT,
	tag               TEXT,
	cycles_count      COUNTER,
	PRIMARY KEY ((name), tag)
);

-- Timeseries a la Timber
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
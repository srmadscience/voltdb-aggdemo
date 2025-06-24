--
-- Copyright (C) 2025 Volt Active Data Inc.
--
-- Use of this source code is governed by an MIT
-- license that can be found in the LICENSE file or at
-- https://opensource.org/licenses/MIT.
--

LOAD CLASSES ../jars/voltdb-aggdemo.jar;

file -inlinebatch END_OF_BATCH

CREATE FUNCTION getHighestValidSequence FROM METHOD mediationdemo.MediationRecordSequenceObserver.getHighestValidSequence;

CREATE FUNCTION sequenceToString FROM METHOD mediationdemo.MediationRecordSequenceObserver.getSeqnosAsText;

CREATE TABLE mediation_parameters 
(parameter_name varchar(30) not null primary key
,parameter_value bigint not null);

CREATE TABLE cdr_dupcheck
(	 sessionId bigint not null,
	 sessionStartUTC timestamp not null,
	 callingNumber varchar(20) ,
	 used_seqno_array varbinary(32),
	 insert_date timestamp not null,
	 agg_state varchar(10),
	 last_agg_date timestamp ,
	 aggregated_usage bigint default 0,
	 unaggregated_usage bigint default 0,
	 max_input_lag_ms bigint default 0,
	 primary key (sessionId,sessionStartUTC)
)
USING TTL 25 HOURS ON COLUMN insert_date BATCH_SIZE 50000;

PARTITION TABLE cdr_dupcheck ON COLUMN sessionId;

CREATE INDEX cdd_ix1 ON cdr_dupcheck (insert_date);

CREATE VIEW cdr_dupcheck_agg_summary_minute AS
SELECT truncate(MINUTE, last_agg_date) last_agg_date, agg_state, count(*) how_many, sum(aggregated_usage) aggregated_usage
FROM cdr_dupcheck
GROUP BY  truncate(MINUTE, last_agg_date) , agg_state;

CREATE INDEX cdasm_ix1 ON cdr_dupcheck_agg_summary_minute (last_agg_date);

CREATE VIEW cdr_dupcheck_session_summary_minute AS
SELECT truncate(MINUTE, sessionStartUTC) sessionStartUTC, agg_state, count(*) how_many, sum(aggregated_usage) aggregated_usage
FROM cdr_dupcheck
GROUP BY  truncate(MINUTE, sessionStartUTC) , agg_state;

CREATE INDEX cdssm_ix1 ON cdr_dupcheck_session_summary_minute (sessionStartUTC);

CREATE view cdr_processing_lag AS
SELECT truncate(MINUTE, insert_date) insert_date
     , max_input_lag_ms lag_ms
     , count(*) how_many
FROM cdr_dupcheck
GROUP BY truncate(MINUTE, insert_date) 
     , max_input_lag_ms ;


CREATE VIEW total_unaggregated_usage AS 
SELECT sum(unaggregated_usage) unaggregated_usage
FROM cdr_dupcheck;


CREATE STREAM bad_cdrs  
EXPORT TO TARGET bad_cdrs 
PARTITION ON COLUMN sessionId 
(	 reason varchar(10) not null,
     sessionId bigint not null,
	 sessionStartUTC timestamp not null,
	 seqno bigint not null,
	 end_seqno bigint, 
	 callingNumber varchar(20) ,
	 destination varchar(512) not null,
	 recordType varchar(5) not null,
	 recordStartUTC timestamp not null,
	 end_recordStartUTC timestamp,
	 recordUsage bigint not null
);


CREATE STREAM unaggregated_cdrs
EXPORT TO TARGET unaggregated_cdrs 
PARTITION ON COLUMN sessionId 
(	 sessionId bigint not null,
	 sessionStartUTC timestamp not null,
	 seqno bigint not null,
	 callingNumber varchar(20) ,
	 destination varchar(512) not null,
	 recordType varchar(1) not null,
	 recordStartUTC timestamp not null,
	 recordUsage bigint not null,
);


CREATE VIEW unaggregated_cdrs_by_session AS
SELECT sessionId, sessionStartUTC
     , min(recordStartUTC) min_recordStartUTC
     , max(recordStartUTC) max_recordStartUTC
     , min(seqno) min_seqno
     , max(seqno) max_seqno
     , sum(recordUsage) recordUsage
     , max(callingNumber) callingNumber
     , max(destination) destination
     , count(*) how_many 
FROM unaggregated_cdrs
GROUP BY sessionId, sessionStartUTC;

CREATE INDEX ucbs_ix1 ON unaggregated_cdrs_by_session
(min_recordStartUTC, sessionId, sessionStartUTC);

CREATE STREAM aggregated_cdrs 
EXPORT TO TARGET aggregated_cdrs 
PARTITION ON COLUMN sessionId
(	 reason varchar(10) not null,
     sessionId bigint not null,
	 sessionStartUTC timestamp not null,
	 min_seqno bigint not null,
	 max_seqno bigint not null,
	 callingNumber varchar(20) ,
	 destination varchar(512) not null,
	 startAggTimeUTC timestamp not null,
	 endAggTimeUTC timestamp not null,
	 recordUsage bigint not null,
	 agg_date timestamp default now
);


CREATE PROCEDURE  
   PARTITION ON TABLE cdr_dupcheck COLUMN sessionid
   FROM CLASS mediationdemo.GetBySessionId;  
   
DROP PROCEDURE HandleMediationCDR IF EXISTS;

CREATE PROCEDURE  
   PARTITION ON TABLE cdr_dupcheck COLUMN sessionid
   FROM CLASS mediationdemo.HandleMediationCDR;  
   
DROP PROCEDURE FlushStaleSessions IF EXISTS;

CREATE PROCEDURE DIRECTED
   FROM CLASS mediationdemo.FlushStaleSessions;  
   
CREATE TASK FlushStaleSessionsTask
ON SCHEDULE  EVERY 1 SECONDS
PROCEDURE FlushStaleSessions
ON ERROR LOG 
RUN ON PARTITIONS;

create procedure get_processing_lag as 
select lag_ms, how_many 
from cdr_processing_lag 
where insert_date = truncate(minute, DATEADD(MINUTE, -1, NOW)) 
order by insert_date, lag_ms;


CREATE PROCEDURE ShowAggStatus__promBL AS
BEGIN
select 'mediation_agg_state_unaggregated_sessions' statname
     ,  'mediation_agg_state_unaggregated_sessions' stathelp  
     , how_many statvalue 
from cdr_dupcheck_agg_summary_minute where last_agg_date IS null;

select 'mediation_agg_state_unaggregated_usage' statname
     ,  'mediation_agg_state_unaggregated_usage' stathelp  
     , nvl(UNAGGREGATED_USAGE,0)  statvalue 
from total_unaggregated_usage;






select 'mediation_parameter_'||parameter_name statname
     ,  'mediation_parameter_'||parameter_name stathelp  
     , parameter_value statvalue 
from mediation_parameters order by parameter_name;

SELECT 'current_agg_lag_ms' statname, 'current_agg_lag_ms' stathelp,
since_epoch(Millisecond, now) - since_epoch(Millisecond,min_recordStartUTC) statvalue
FROM unaggregated_cdrs_by_session ORDER BY min_recordStartUTC LIMIT 1;

END;

END_OF_BATCH



upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('AGG_USAGE',1000000);

upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('AGG_QTYCOUNT',100);

upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('STALENESS_THRESHOLD_MS',3600000);

upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('AGG_WINDOW_SIZE_MS',4000);

upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('DUPCHECK_TTLMINUTES',1440);

upsert into mediation_parameters
(parameter_name ,parameter_value)
VALUES
('STALENESS_ROWLIMIT',1000);


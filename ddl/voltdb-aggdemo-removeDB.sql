--
-- Copyright (C) 2025 Volt Active Data Inc.
--
-- Use of this source code is governed by an MIT
-- license that can be found in the LICENSE file or at
-- https://opensource.org/licenses/MIT.
--



DROP TASK FlushStaleSessionsTask IF EXISTS;

DROP PROCEDURE GetBySessionId IF EXISTS; 

DROP PROCEDURE HandleMediationCDR IF EXISTS; 
   
DROP PROCEDURE FlushStaleSessions IF EXISTS;

DROP PROCEDURE ShowAggStatus__promBL IF EXISTS;

DROP PROCEDURE get_processing_lag if exists;

DROP VIEW cdr_processing_lag if exists;

DROP VIEW total_unaggregated_usage IF EXISTS;

DROP VIEW cdr_dupcheck_agg_summary_minute IF EXISTS;

DROP VIEW cdr_dupcheck_session_summary_minute IF EXISTS;

DROP VIEW unaggregated_cdrs_by_session IF EXISTS;

DROP VIEW agg_summary_view IF EXISTS;

DROP TABLE mediation_parameters IF EXISTS;

DROP TABLE cdr_dupcheck IF EXISTS;

DROP STREAM bad_cdrs IF EXISTS;

DROP STREAM aggregated_cdrs IF EXISTS;

DROP STREAM unaggregated_cdrs IF EXISTS;

DROP FUNCTION getHighestValidSequence IF EXISTS;

DROP FUNCTION sequenceToString IF EXISTS;



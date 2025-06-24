/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package mediationdemo;


import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Abstract procedure class that contains shared aggregation and cancellation functionality.
 *
 */
public abstract class AbstractMediationProcedure extends VoltProcedure {

    public static final long ONE_WEEK_IN_MS = 1000 * 60 * 60 * 24 * 7;

    // @formatter:off

	public static final SQLStmt getParameter = new SQLStmt(
			"SELECT parameter_value FROM mediation_parameters WHERE parameter_name = ? ;");
		
	public static final SQLStmt deleteSessionRunningTotals = new SQLStmt(
				"DELETE FROM unaggregated_cdrs_by_session WHERE sessionId = ? AND sessionStartUTC = ?;");
			
	public static final SQLStmt createAggregatedSession = new SQLStmt(
					"INSERT INTO aggregated_cdrs  " +
							"( reason, sessionId, " +
							" sessionStartUTC, " +
							" min_seqno, max_seqno, " +
							" callingNumber, " +
							" destination, " +
							" startAggTimeUTC, endAggTimeUTC," +
							" recordUsage)  " +
							"VALUES " +
							"(?,?,?, ?,?,?, ?,?,?, ?); ");
		
	public static final SQLStmt reportBadRange = new SQLStmt(
				"INSERT INTO bad_cdrs  " +
						"( reason, sessionId, " +
						" sessionStartUTC, " +
						" seqno,end_seqno, " +
						" callingNumber, " +
						" destination, " +
						" recordType, " +
						" recordStartUTC, end_recordStartUTC, " +
						" recordUsage)  " +
						"VALUES " +
						"(?,?,?,?,?,?,?,?,?,?,?); ");
		
	public static final SQLStmt updateAggStatus = new SQLStmt(
				"UPDATE cdr_dupcheck SET last_agg_date = NOW, agg_state = ?"
				+ ", aggregated_usage = aggregated_usage + ?"
				+ ", unaggregated_usage = 0 "
				+ ", max_input_lag_ms = " + ONE_WEEK_IN_MS + " "
				+ "WHERE sessionId = ? AND sessionStartUTC = ?;");	
	
	// @formatter:on


	protected static final String AGG_USAGE = "AGG_USAGE";
	protected static final String AGG_QTYCOUNT = "AGG_QTYCOUNT";
	protected static final String STALENESS_THRESHOLD_MS = "STALENESS_THRESHOLD_MS";
	protected static final String AGG_WINDOW_SIZE_MS = "AGG_WINDOW_SIZE_MS";
	protected static final Object STALENESS_ROWLIMIT = "STALENESS_ROWLIMIT";
	


	/**
	 * Aggregate a session. We assume that totalRecordsTable is currently on the right row.
	 * @param totalRecordsTable
	 * @param aggReason
	 */
	protected void aggregateSession(VoltTable totalRecordsTable, String aggReason) {

		// Unload data from record
		long minSeqno = totalRecordsTable.getLong("min_seqno");
		long maxSeqno = totalRecordsTable.getLong("max_seqno");
		TimestampType startDate = totalRecordsTable.getTimestampAsTimestamp("min_recordStartUTC");
		TimestampType endDate = totalRecordsTable.getTimestampAsTimestamp("max_recordStartUTC");
		long sessionId = totalRecordsTable.getLong("sessionId");
		TimestampType sessionStartUTC = totalRecordsTable.getTimestampAsTimestamp("sessionStartUTC");
		String callingNumber = totalRecordsTable.getString("callingNumber");
		String destination = totalRecordsTable.getString("destination");
		long unaggedRecordUsageToReport = totalRecordsTable.getLong("recordUsage");

		//Create an aggregated session
		voltQueueSQL(createAggregatedSession, aggReason, sessionId, sessionStartUTC, minSeqno, maxSeqno, callingNumber,
				destination, startDate, endDate, unaggedRecordUsageToReport);
		
		// Report change in status
		voltQueueSQL(updateAggStatus, aggReason, unaggedRecordUsageToReport, sessionId, sessionStartUTC);
		
		// Delete unneeded records
		deleteSessionRunningTotals(sessionId, sessionStartUTC);
	}

	/**
	 * Cancel a late session. We assume that totalRecordsTable is currently on the right row.
	 * @param sessionToClose
	 */
	protected void cancelLateSession(VoltTable sessionToClose) {

		// Unload data from record
		long sessionId = sessionToClose.getLong("sessionId");
		TimestampType sessionStartUTC = sessionToClose.getTimestampAsTimestamp("sessionStartUTC");
		long minSeqno = sessionToClose.getLong("min_seqno");
		long maxSeqno = sessionToClose.getLong("max_seqno");
		TimestampType startDate = sessionToClose.getTimestampAsTimestamp("min_recordStartUTC");
		TimestampType endDate = sessionToClose.getTimestampAsTimestamp("max_recordStartUTC");
		String callingNumber = sessionToClose.getString("callingNumber");
		String destination = sessionToClose.getString("destination");
		long unaggedRecordUsageToReport = sessionToClose.getLong("recordUsage");

		// Cancel session
		voltQueueSQL(reportBadRange, "LATE", sessionId, sessionStartUTC, minSeqno, maxSeqno, callingNumber, destination,
				"RANGE", startDate, endDate, unaggedRecordUsageToReport);
		
		// Report change in status
		voltQueueSQL(updateAggStatus, "LATE", 0, sessionId,  sessionStartUTC);

		// Delete unneeded records
		deleteSessionRunningTotals(sessionId, sessionStartUTC);
	}

	/**
	 * Delete a row from the stream view unaggregated_cdrs_by_session. Being a stream
	 * view this doesn't happen by itself.
	 * @param sessionId
	 * @param sessionStartUTC
	 */
	protected void deleteSessionRunningTotals(long sessionId, TimestampType sessionStartUTC) {
		voltQueueSQL(deleteSessionRunningTotals, sessionId, sessionStartUTC);
	}

	/**
	 * Get parameter at current row in voltTable, or return defaultValue
	 * if not found.
	 * 
	 * @param voltTable
	 * @param defaultValue
	 * @return
	 */
	protected long getParameterIfSet(VoltTable voltTable, long defaultValue) {

		if (voltTable.advanceRow()) {
			return (voltTable.getLong("parameter_value"));
		}

		return defaultValue;
	}

}

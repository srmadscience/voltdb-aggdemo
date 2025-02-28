/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package mediationdemo;


import java.util.Date;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Take an incoming Kafka message and aggregate it.
 *
 */
public class HandleMediationCDR extends AbstractMediationProcedure {

	// @formatter:off

	public static final SQLStmt getSession = new SQLStmt(
			"SELECT * FROM cdr_dupcheck WHERE sessionId = ? AND sessionStartUTC = ?;");

	public static final SQLStmt getSessionRunningTotals = new SQLStmt(
			"SELECT * FROM unaggregated_cdrs_by_session WHERE sessionId = ? AND sessionStartUTC = ?;");

	public static final SQLStmt updateSessionSeqnosAndUsage = new SQLStmt(
				"UPDATE cdr_dupcheck SET used_seqno_array = ?, "
				+ "unaggregated_usage = unaggregated_usage + ?, "
				+ "max_input_lag_ms = ? "
				+ "WHERE sessionId = ? "
				+ "AND sessionStartUTC = ?;");

	public static final SQLStmt createSession = new SQLStmt(
				"INSERT INTO cdr_dupcheck "
				+ "(sessionId  , sessionStartUTC  , callingNumber , used_seqno_array "
				+ ", unaggregated_usage, max_input_lag_ms, insert_date)"
				+ " VALUES "
				+ "(?,?,?,?,?,?, NOW)");
		
	public static final SQLStmt createUnaggedRecordSession = new SQLStmt(
				"INSERT INTO unaggregated_cdrs  " +
						"( sessionId, " +
						" sessionStartUTC, " +
						" seqno, " +
						" callingNumber, " +
						" destination, " +
						" recordType, " +
						" recordStartUTC, " +
						" recordUsage)  " +
						"VALUES " +
						"(?,?,?,?,?,?,?,?); ");

	public static final SQLStmt reportBadRecord = new SQLStmt(
				"INSERT INTO bad_cdrs  " +
						"( reason, sessionId, " +
						" sessionStartUTC, " +
						" seqno, " +
						" callingNumber, " +
						" destination, " +
						" recordType, " +
						" recordStartUTC, " +
						" recordUsage)  " +
						"VALUES " +
						"(?,?,?,?,?,?,?,?,?); ");
		
	// @formatter:on

	protected static final long ONE_WEEK_IN_MS = 1000 * 60 * 60 * 24 * 7;

	protected long aggQtyThreshold = 50;
	protected long aggUsageThreshold = 1000000;

	public VoltTable[] run(long sessionId, long sessionStartUTC, int seqno, String callingNumber, String destination,
			String recordType, long recordStartUTC, long recordUsage) throws VoltAbortException {

		// We refuse to process anything that shows up more than 1 week late...
		final Date cutoffDate = new Date(this.getTransactionTime().getTime() - ONE_WEEK_IN_MS);
		final Date sessionStartUTCAsDate = new Date(sessionStartUTC);
		final Date recordStartUTCAsDate = new Date(recordStartUTC);

		if (sessionStartUTCAsDate.before(cutoffDate)) {
			// This is dated more than 1 week ago - reject...
			voltQueueSQL(reportBadRecord, "LATESESSION", sessionId, sessionStartUTC, seqno, callingNumber, destination,
					recordType, recordStartUTCAsDate, recordUsage);
			return(getEmptyVoltTables());
		}

		if (recordStartUTCAsDate.before(cutoffDate)) {
			// This is dated more than 1 week ago - reject...
			voltQueueSQL(reportBadRecord, "LATERECORD", sessionId, sessionStartUTC, seqno, callingNumber, destination,
					recordType, recordStartUTCAsDate, recordUsage);
			return(getEmptyVoltTables());
		}

		// See if we know about this session, and find out what our
		// parameters are...
		voltQueueSQL(getSession, sessionId, sessionStartUTCAsDate);
		voltQueueSQL(getParameter, AGG_USAGE);
		voltQueueSQL(getParameter, AGG_QTYCOUNT);

		VoltTable[] sessionRecords = voltExecuteSQL();

		VoltTable sessionDupCheck = sessionRecords[0];
		aggUsageThreshold = getParameterIfSet(sessionRecords[1], aggUsageThreshold);
		aggQtyThreshold = getParameterIfSet(sessionRecords[2], aggQtyThreshold);

		// We use this to store all the sequence numbers we've seen for this session
		// instead of storing one row per sequence nuumber....
		MediationRecordSequence msr = new MediationRecordSequence(null);

		// Do duplicate checking...
		if (sessionDupCheck.advanceRow()) {

			// We've seen at least one row for this session...

			// Unload dup check record so we can see what it is
			msr = new MediationRecordSequence(sessionDupCheck.getVarbinary("used_seqno_array"));
			boolean ourSeqIsSet = msr.getSeqno(seqno);

			if (ourSeqIsSet) {
				// This is a dup - reject...
				voltQueueSQL(reportBadRecord, "DUP", sessionId, sessionStartUTC, seqno, callingNumber, destination,
						recordType, recordStartUTCAsDate, recordUsage);
				return(getEmptyVoltTables());
			}

			// Note we've see this seqno
			msr.setSeqno(seqno);
			long maxLag = sessionDupCheck.getLong("max_input_lag_ms");
			long latestLag = this.getTransactionTime().getTime() - recordStartUTC;
			
			if (latestLag > maxLag) {
			    maxLag = latestLag; 
			}
			voltQueueSQL(updateSessionSeqnosAndUsage, msr.getSequence(), recordUsage, maxLag, sessionId, sessionStartUTCAsDate);

		} else {

			// New session we've never heard of..
			msr.setSeqno(seqno);
			voltQueueSQL(createSession, sessionId, sessionStartUTCAsDate, callingNumber, msr.getSequence(),recordUsage, (this.getTransactionTime().getTime() - recordStartUTC));

		}

		// Add message contents to running totals, and then see whether we can
		// aggregate...
		voltQueueSQL(createUnaggedRecordSession, sessionId, sessionStartUTCAsDate, seqno, callingNumber, destination,
				recordType, recordStartUTCAsDate, recordUsage);
		voltQueueSQL(getSessionRunningTotals, sessionId, sessionStartUTCAsDate);

		VoltTable[] totalRecords = voltExecuteSQL();
		VoltTable totalRecordsTable = totalRecords[totalRecords.length - 1];

		aggregateSessionIfNeeded(totalRecordsTable, msr, recordType, seqno,recordStartUTCAsDate);

		
		return(getEmptyVoltTables());
	}

	protected void aggregateSessionIfNeeded(VoltTable totalRecordsTable, MediationRecordSequence msr, String recordType,
			int seqno, Date recordStartUTCAsDate) {
		totalRecordsTable.advanceRow();

		long unaggedRecordCount = totalRecordsTable.getLong("how_many");
		long unaggedRecordUsage = totalRecordsTable.getLong("recordUsage");
		long sessionId = totalRecordsTable.getLong("sessionId");
		TimestampType sessionStartUTC = totalRecordsTable.getTimestampAsTimestamp("sessionStartUTC");

		// See if we have a complete set of records...		
		if (msr.weHaveFromZeroTo(seqno)) {

			String aggReason = null;

			// Decide whether to aggregate this session
			if (recordType.equalsIgnoreCase("E")) {
				aggReason = "END";
			} else if (unaggedRecordCount >= aggQtyThreshold) {
				aggReason = "QTY";
			} else if (unaggedRecordUsage >= aggUsageThreshold) {
				aggReason = "USAGE";
			}

			if (aggReason != null) {

				aggregateSession(totalRecordsTable, aggReason);

				deleteSessionRunningTotals(sessionId, sessionStartUTC);

				voltExecuteSQL();
			}

		}
	}
	
	private VoltTable[] getEmptyVoltTables() {
		
		voltExecuteSQL();
		return new VoltTable[0];
	}

}

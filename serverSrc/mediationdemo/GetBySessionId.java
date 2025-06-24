/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package mediationdemo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Return information about a session
 */
public class GetBySessionId extends AbstractMediationProcedure {

	// @formatter:off

	public static final SQLStmt getSession = new SQLStmt(
			"SELECT d.*, sequenceToString(used_seqno_array) seqnos_used "
			+ ", getHighestValidSequence(used_seqno_array) highest_valid_seqno "
			+ "FROM cdr_dupcheck d "
			+ "WHERE sessionId = ? AND sessionStartUTC = ?;");

	public static final SQLStmt getSessionRunningTotals = new SQLStmt(
			"SELECT * FROM unaggregated_cdrs_by_session WHERE sessionId = ? AND sessionStartUTC = ?;");
			
	// @formatter:on

	public VoltTable[] run(long sessionId, TimestampType sessionStartUTC) throws VoltAbortException {

		voltQueueSQL(getSession, sessionId, sessionStartUTC);
		voltQueueSQL(getSessionRunningTotals, sessionId, sessionStartUTC);

		return voltExecuteSQL(true);

	}

}

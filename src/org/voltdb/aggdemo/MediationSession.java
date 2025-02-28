/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;


import java.util.Random;

/**
 * Class representing a user session in our mediation demo.
 *
 */
public class MediationSession {

	public static final String SESSION_START = "S";
	public static final String SESSION_INTERMEDIATE = "I";
	public static final String SESSION_END = "E";
	public static final int MAX_POSSIBLE_SEQNO = 255;

	long sessionStartUTC = System.currentTimeMillis();
	String callingNumber;
	String destination;
	public long sessionid;
	int seqno;
	int maxSeqno;

	/**
	 * Because we expect to have millions of MediationSessions at the same time we
	 * share an instance of Random.
	 */
	Random r;

	/**
	 * Create a simulated device that will produce different kinds of CDRS...
	 * 
	 * @param callingNumber - Our device ID
	 * @param destination   - A web site the device is speaking to
	 * @param sessionid     - A Unique ID
	 * @param r             - An instance of Random the MediationSessions share
	 */
	public MediationSession(String callingNumber, String destination, long sessionid, Random r) {
		super();
		this.callingNumber = callingNumber;
		this.destination = destination;
		this.sessionid = sessionid;
		this.callingNumber = callingNumber;
		this.r = r;
		seqno = 0;
		maxSeqno = r.nextInt(MAX_POSSIBLE_SEQNO);
	}

	/**
	 * 
	 * Calling this method advances the state of this session. A session state
	 * begins with SESSION_START, then has an arbitrary number of
	 * SESSION_INTERMEDIATE's and finally a SESSION_END. Each time we are called we
	 * increment seqno. Just to be difficult the returned CDR only has the
	 * callingNumber when it's a SESSION_START
	 * 
	 * @return A new, unique, CDR
	 */
	public MediationMessage getNextCdr() {

		MediationMessage newCDR = new MediationMessage(sessionid, sessionStartUTC, seqno, null, destination);
		newCDR.setRecordStartUTC(System.currentTimeMillis());

		switch (seqno) {
		case 0:
			// Note that this is the *only* time we identify the phone
			newCDR.setCallingNumber(callingNumber);
			newCDR.setEventType(SESSION_START);
			newCDR.setRecordUsage(r.nextInt(100000));
			seqno++;
			break;

		case MAX_POSSIBLE_SEQNO:

			newCDR.setEventType(SESSION_END);
			newCDR.setRecordUsage(r.nextInt(100));
			sessionStartUTC = System.currentTimeMillis();
			seqno = 0;
			break;

		default:

			newCDR.setEventType(SESSION_INTERMEDIATE);
			newCDR.setRecordUsage(r.nextInt(100000));
			seqno++;
		}

		return newCDR;
	}

}

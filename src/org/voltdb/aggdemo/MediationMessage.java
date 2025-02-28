/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;


/**
 * A class representing a message generated by network devices in
 * our mediation demo
 *
 */
public class MediationMessage {
	
	private long sessionId;
	private long sessionStartUTC;
	private long seqno;	
	private String callingNumber;
	private String destination;
	private String eventType;	
	private long recordStartUTC;
	private long recordUsage;

	public MediationMessage(long sessionId, long sessionStartUTC, long seqno, String callingNumber, String destination)  {
		super();
		this.sessionId = sessionId;
		this.sessionStartUTC = sessionStartUTC;
		this.seqno = seqno;
		this.callingNumber = callingNumber;
		this.destination = destination;
	}

	/**
	 * @return the sessionId
	 */
	public long getSessionId() {
		return sessionId;
	}

	/**
	 * @return the sessionStartUTC
	 */
	public long getSessionStartUTC() {
		return sessionStartUTC;
	}

	/**
	 * @return the seqno
	 */
	public long getSeqno() {
		return seqno;
	}

	/**
	 * @return the callingNumber
	 */
	public String getCallingNumber() {
		return callingNumber;
	}

	/**
	 * @return the destination
	 */
	public String getDestination() {
		return destination;
	}

	/**
	 * @return the eventType
	 */
	public String getEventType() {
		return eventType;
	}


	/**
	 * @return the recordStartUTC
	 */
	public long getRecordStartUTC() {
		return recordStartUTC;
	}

	/**
	 * @return the recordUsage
	 */
	public long getRecordUsage() {
		return recordUsage;
	}

	/**
	 * @param eventType the eventType to set
	 */
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}


	/**
	 * @param recordStartUTC the recordStartUTC to set
	 */
	public void setRecordStartUTC(long recordStartUTC) {
		this.recordStartUTC = recordStartUTC;
	}

	/**
	 * @param recordUsage the recordUsage to set
	 */
	public void setRecordUsage(long recordUsage) {
		this.recordUsage = recordUsage;
	}
	
	/**
	 * @param callingNumber the callingNumber to set
	 */
	public void setCallingNumber(String callingNumber) {
		this.callingNumber = callingNumber;
	}


	@Override
	public String toString() {
		
		StringBuilder builder = new StringBuilder();
		builder.append(sessionId);
		builder.append(",");
		builder.append(sessionStartUTC);
		builder.append(",");
		builder.append(seqno);
		builder.append(",");
		builder.append(callingNumber);
		builder.append(",");
		builder.append(destination);
		builder.append(",");
		builder.append(eventType);
		builder.append(",");
		builder.append(recordStartUTC);
		builder.append(",");
		builder.append(recordUsage);
		return builder.toString();
	}


	

}

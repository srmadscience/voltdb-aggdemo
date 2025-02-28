/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package mediationdemo;



import java.util.BitSet;

import org.voltdb.aggdemo.MediationSession;

/**
 * This is a BitSet where each Bit in each byte represents a possible sequence number
 * for a Mediation Session. 
 * 
 * If you haven't worked with BitSet before https://www.baeldung.com/java-bitset
 * is a useful reference.
 *
 */
public class MediationRecordSequence {

	BitSet theBitSet;

	/**
	 * Create a Bitset from an existing byte array. If it's null create
	 * one with MediationSession.MAX_POSSIBLE_SEQNO entries.
	 * @param rawData
	 */
	public MediationRecordSequence(byte[] rawData) {

		if (rawData == null) {
			theBitSet = new BitSet(MediationSession.MAX_POSSIBLE_SEQNO);
		} else {
			theBitSet = BitSet.valueOf(rawData);
		}

	}

	/**
	 * Mark seqno as having been seen.
	 * 
	 * @param seqno
	 */
	public void setSeqno(int seqno) {
		theBitSet.set(seqno);
	}

	/**
	 * See if a seqno has been seen.
	 * @param seqno
	 * @return true if 'seqno' has been seen, otherwise false.
	 */
	public boolean getSeqno(int seqno) {
		return theBitSet.get(seqno);
	}

	/**
	 * See if we have a full range from zero to seqno. Useful
	 * for checking for missing records prior to aggregation.
	 * @param seqno
	 * @return true if we have a full set.
	 */
	public boolean weHaveFromZeroTo(int seqno) {

		for (int i = 0; i < seqno; i++) {
			if (!theBitSet.get(i)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Convert back to byte[] for storage.
	 * @return BitSet as byte[]
	 */
	public byte[] getSequence() {
		return theBitSet.toByteArray();
	}

	@Override
	public String toString() {
		
		StringBuffer allString = new StringBuffer(MediationSession.MAX_POSSIBLE_SEQNO+1);
		
		for (int i=0; i < theBitSet.length(); i++) {
			if (getSeqno(i)) {
				allString.append('X');
			} else {
				allString.append('_');
			}
		}
				
		return allString.toString();
	}

}

/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package mediationdemo;

import org.voltdb.VoltProcedure.VoltAbortException;


/**
 * This class is used by the Volt server functions getHighestValidSequencand
 * sequenceToString
 *
 */
public class MediationRecordSequenceObserver {

    /**
     * Returns the highest value filled in from zero to 'value', if all values are
     * filled in. So:
     * <ul>
     * <li>If bits 0,1,2 are filled in it returns '2'.</li>
     * <li>If bits 0,1,2,5 are filled in it returns -1.</li>
     * </ul>
     * <p>
     * This is used by the SQL function getHighestValidSequence, which is used by
     * GetBySessionId and MediationRecordSequenceObserver
     * 
     * @param varbinaryArray a byte array containing a java.util.BitSet
     * @return the highest element if and only if all prior elements filled in.
     * @throws VoltAbortException - makes sure any Exceptions won't hurt Volt
     */
    public int getHighestValidSequence(byte[] varbinaryArray) throws VoltAbortException {

        try {
            MediationRecordSequence msr = new MediationRecordSequence(varbinaryArray);

            int seqnoCount = msr.theBitSet.cardinality();

            if (seqnoCount == 0) {
                // No bits are set..
                return -1;
            }

            if (msr.weHaveFromZeroTo(seqnoCount - 1)) {
                // All of the bits up to the highest one are set.
                return seqnoCount - 1;
            }

            return -1;

        } catch (Exception e) {
            throw new VoltAbortException(e);
        }

    }

    /**
     * Takes a byte[] containing a java.util.BitSet and returns a text
     * representation.
     * <p>
     * This is used by the SQL function sequenceToString, which is used by
     * MediationRecordSequenceObserver
     * <p> 
     * <ul>
     * <li>A contiguous block from 0 to 4 would show return '0-4'.</li>
     * <li>0,1 and 4 would return 'XX_X'.
     * </ul>
     * @param varbinaryArray a java.util.BitSet
     * @return A Text representation.
     * @throws VoltAbortException - makes sure any Exceptions won't hurt Volt
     */
    public String getSeqnosAsText(byte[] varbinaryArray) {

        try {
            int highestValidSequence = getHighestValidSequence(varbinaryArray);
            StringBuffer b = new StringBuffer();

            if (highestValidSequence > -1) {

                b.append("0-");
                b.append(highestValidSequence);
                return b.toString();

            }

            MediationRecordSequence msr = new MediationRecordSequence(varbinaryArray);

            return (msr.toString());
            
        } catch (Exception e) {
            throw new VoltAbortException(e);
        }

    }
}

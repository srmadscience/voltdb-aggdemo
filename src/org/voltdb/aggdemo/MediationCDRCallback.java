/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;

import java.util.concurrent.BlockingQueue;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

/**
 * Simple callback that complains if something went badly wrong.
 *
 */
public class MediationCDRCallback implements ProcedureCallback {

    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();
    ActiveSession pseudoRandomSession;
    BlockingQueue<ActiveSession> burstingSessionQueue;

    public MediationCDRCallback(ActiveSession pseudoRandomSession, BlockingQueue<ActiveSession> burstingSessionQueue) {
        this.pseudoRandomSession = pseudoRandomSession;
        this.burstingSessionQueue = burstingSessionQueue;
    }

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {

        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            MediationDataGenerator.msg("Error Code " + arg0.getStatusString());
        } else {
            try {
                if (pseudoRandomSession != null) {
                    if (pseudoRandomSession.getAndDecrementRemainingActvity() > 0) {

                        // Add entry back to queue if we're not finished with it....
                        burstingSessionQueue.add(pseudoRandomSession);
                        shc.incCounter(MediationDataGenerator.SESSION_RETURNED_TO_QUEUE);

                    } else {
                        shc.incCounter(MediationDataGenerator.SESSION_ENDED);
                    }
                }

            } catch (IllegalStateException e) {
                shc.incCounter(MediationDataGenerator.SESSION_QUEUE_FULL);
            }

        }
    }

}

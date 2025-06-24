/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.voltdb.voltutil.stats.SafeHistogramCache;

/**
 * Simple callback that complains if something went badly wrong.
 *
 */
public class MediationCDRKafkaCallback implements org.apache.kafka.clients.producer.Callback {

    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();
    ActiveSession pseudoRandomSession;
    BlockingQueue<ActiveSession> burstingSessionQueue;

    public MediationCDRKafkaCallback(ActiveSession pseudoRandomSession,
            BlockingQueue<ActiveSession> burstingSessionQueue) {
        this.pseudoRandomSession = pseudoRandomSession;
        this.burstingSessionQueue = burstingSessionQueue;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, java.lang.Exception exception) {
        if (exception != null) {
            MediationDataGenerator.msg("MediationCDRKafkaCallback:" + exception.getMessage());
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

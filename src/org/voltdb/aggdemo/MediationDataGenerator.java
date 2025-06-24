/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import mediationdemo.AbstractMediationProcedure;


/**
 * This generates mock CDRS that need to be aggregated. It also deliberately
 * introduces the same kind of mistakes we see in the real world, such as
 * duplicate records, missing records and late records.
 *
 */
public class MediationDataGenerator {

    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";

    private static final String NUM_PREFIX = "Num";

    /**
     * Max time we track for stats purposes
     */
    private static final int MAX_LAG_MS = 10000;

    /**
     * Name of stat we store
     */
    public static final String INPUT_LAG = "InputLag";

    /**
     * Name of stat we store
     */
    public static final String OUTPUT_LAG = "OutputLag";

    public static final String INPUT_CLIENT_LAG = "InputClientLag";

    public static final String OUTPUT_POLL_BATCHSIZE = "OutputPollBatchSize";

    public static final int INPUT_LAG_HISTOGRAM_SIZE = 600000;

    public static final int OUTPUT_LAG_HISTOGRAM_SIZE = 600000;

    public static final int BURST_SESSION_BATCH_SIZE = 10;

    public static final String SESSION_PULLED_FROM_Q = "SESSION_PULLED_FROM_Q";

    public static final String SESSION_IS_NEW = "SESSION_IS_NEW";

    public static final String SESSION_QUEUE_FULL = "SESSION_QUEUE_FULL";

    public static final String SESSION_RETURNED_TO_QUEUE = "SESSION_RETURNED_TO_QUEUE";

    private static final String SESSION_Q_EMPTY = "SESSION_Q_EMPTY";

    public static final String SESSION_ENDED = "SESSION_ENDED";

    public static final String KAFKA_PARTITONER_NAME = "KAFKA_PARTITONER_NAME";

    private static final int WRITE_TO_KAFKA_WITH_CONSUMER = 1;
    private static final int WRITE_TO_KAFKA_BUT_NO_CONSUMER = 2;

    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    Client voltClient = null;

    Producer<Long, MediationMessage> producer = null;

    AggregatedRecordConsumer arc = null;

    String hostnames;
    String kafkaHostnames;
    int userCount;
    int tpMs;
    int durationSeconds;

    int missingRatio;
    int dupRatio;
    int lateRatio;
    int dateis1970Ratio;

    int normalCDRCount = 0;
    int missingCount = 0;
    int dupCount = 0;
    int lateCount = 0;
    int normalCD = 0;
    int dateis1970Count = 0;

    int kafkaPort = 9092;
    int maxActiveSessions;

    HashMap<String, MediationSession> sessionMap = new HashMap<>();
    ArrayList<MediationMessage> dupMessages = new ArrayList<>();
    ArrayList<MediationMessage> lateMessages = new ArrayList<>();

    long startMs;
    Random r = new Random();
    long sessionId = 0;

    long dupCheckTtlMinutes = 3600;

    BlockingQueue<ActiveSession> activeSessionQueue;

    /**
     * Set this to false if you want to send CDRS to VoltDB directly..
     */
    boolean useKafka = false;

    public MediationDataGenerator(String hostnames, String kafkaHostnames, int userCount, long dupCheckTtlMinutes,
            int tpMs, int durationSeconds, int missingRatio, int dupRatio, int lateRatio, int dateis1970Ratio,
            int useKafkaFlag, int kafkaPort, int maxActiveSessions) throws Exception {

        this.hostnames = hostnames;
        this.kafkaHostnames = kafkaHostnames;
        this.userCount = userCount;
        this.dupCheckTtlMinutes = dupCheckTtlMinutes;
        this.tpMs = tpMs;
        this.durationSeconds = durationSeconds;
        this.missingRatio = missingRatio;
        this.dupRatio = dupRatio;
        this.lateRatio = lateRatio;
        this.dateis1970Ratio = dateis1970Ratio;

        if (useKafkaFlag == WRITE_TO_KAFKA_WITH_CONSUMER || useKafkaFlag == WRITE_TO_KAFKA_BUT_NO_CONSUMER ) {
            useKafka = true;
        }

        this.kafkaPort = kafkaPort;
        this.maxActiveSessions = maxActiveSessions;

        activeSessionQueue = new LinkedBlockingDeque<>(maxActiveSessions);

        msg("hostnames=" + hostnames + ", kafkaHostnames=" + kafkaHostnames + ",users=" + userCount + ", tpMs=" + tpMs
                + ",durationSeconds=" + durationSeconds);
        msg("missingRatio=" + missingRatio + ", dupRatio=" + dupRatio + ", lateRatio=" + lateRatio
                + ", dateis1970Ratio=" + dateis1970Ratio);
        msg("use Kafka = " + useKafka + ", kafkaPort=" + kafkaPort);
        msg("max active sessions = " + maxActiveSessions);

        msg("Log into VoltDB's Kafka Broker");

        producer = connectToKafka(kafkaHostnames, "org.apache.kafka.common.serialization.LongSerializer",
                "org.voltdb.aggdemo.MediationMessageSerializer", kafkaPort);

        msg("Log into VoltDB");
        voltClient = connectVoltDB(hostnames);

        msg("Create Agg record consumer");
        arc = new AggregatedRecordConsumer(hostnames, kafkaPort);

        msg("Start Agg record consumer");

        if (useKafkaFlag == WRITE_TO_KAFKA_WITH_CONSUMER) {
            
            Thread thread = new Thread(arc, "AggRecordConsumer");
            thread.start();
            msg("Agg record consumer is thread " + thread.getName());
            
        }

    }

    public boolean run(int offset) {

        long laststatstime = System.currentTimeMillis();
        startMs = System.currentTimeMillis();
        setDupcheckTTLMinutes();

        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;
        long recordCount = 0;
        long lastReportedRecordCount = 0;
        double actualTps = 0;

        if (tpMs == 0) {
            // tpMs == 0 if we only want to run the Consumer process
            try {
                Thread.sleep(1000 * durationSeconds, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {

            while (System.currentTimeMillis() < (startMs + (1000 * durationSeconds))) {

                recordCount++;

                ActiveSession pseudoRandomSession = getPseudoRandomSession(offset);
                final String actualNumber = NUM_PREFIX + pseudoRandomSession.getNumber();

                MediationSession ourSession = sessionMap.get(actualNumber);

                if (ourSession == null) {
                    ourSession = new MediationSession(actualNumber, getRandomDestinationId(), +(offset + sessionId++),
                            r);
                    sessionMap.put(actualNumber, ourSession);
                }

                MediationMessage nextCdr = ourSession.getNextCdr();

                // Now decide what to do. We could just send the CDR, but where's the fun in
                // that?

                if (missingRatio > 0 && r.nextInt(missingRatio) == 0) {
                    // Let's just pretend this CDR never happened...
                    missingCount++;
                } else if (dupRatio > 0 && r.nextInt(dupRatio) == 0) {

                    // let's send it. Lot's of times...
                    for (int i = 0; i < 2 + r.nextInt(10); i++) {
                        send(nextCdr, pseudoRandomSession);
                    }

                    // Also add it to a list of dup messages to send again, later...
                    dupMessages.add(nextCdr);
                    dupCount++;

                } else if (lateRatio > 0 && r.nextInt(lateRatio) == 0) {

                    // Add it to a list of late messages to send later...
                    lateMessages.add(nextCdr);
                    lateCount++;

                } else if (dateis1970Ratio > 0 && r.nextInt(dateis1970Ratio) == 0) {

                    // Set date to Jan 1, 1970, and then send it...
                    nextCdr.setRecordStartUTC(0);
                    send(nextCdr, pseudoRandomSession);
                    dateis1970Count++;

                } else {

                    send(nextCdr, pseudoRandomSession);
                    normalCDRCount++;

                }

                if (tpThisMs++ > tpMs) {

                    // but sleep if we're moving too fast...
                    while (currentMs == System.currentTimeMillis()) {
                        try {
                            Thread.sleep(0, 50000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                if (lateMessages.size() > 100000 || dupMessages.size() > 100000) {
                    sendRemainingMessages();
                }

                if (laststatstime + 10000 < System.currentTimeMillis()) {

                    double recordsProcessed = recordCount - lastReportedRecordCount;
                    actualTps = 1000 * (recordsProcessed / (System.currentTimeMillis() - laststatstime));

                    msg("Offset = " + offset + " Record " + recordCount + " TPS=" + (long) actualTps);
                    msg("Active Sessions: " + sessionMap.size());
                    msg("Bursting Sessions: " + activeSessionQueue.size());

                    msg(shc.toString());

                    laststatstime = System.currentTimeMillis();
                    lastReportedRecordCount = recordCount;

                    printApplicationStats(voltClient, nextCdr);
                }

            }

            // Get some stats!
            gatherInputLagStats();

            sendRemainingMessages();
        }

        try {
            voltClient.drain();
            voltClient.close();
        } catch (NoConnectionsException | InterruptedException e) {
            e.printStackTrace();
        }

        arc.stop();

        printStatus(startMs);

        reportRunLatencyStats(tpMs, (long) actualTps);

        // Declare victory if we got >= 90% of requested TPS...
        if (actualTps / (tpMs * 1000) > .9) {
            return true;

        }

        return false;

    }

    /**
     * Returns a pseudo random number. Numbers we are already using will have
     * priority, otherwise random
     *
     * @param offset
     * @return A pseudo random session.
     */
    private ActiveSession getPseudoRandomSession(int offset) {

        ActiveSession b = null;

        try {

            b = activeSessionQueue.poll(0, TimeUnit.MILLISECONDS);

            if (b == null) {
                shc.incCounter(SESSION_Q_EMPTY);
            } else {

                shc.incCounter(SESSION_PULLED_FROM_Q);

                return new ActiveSession(b.getNumber() + offset, b.remainingActvity);
            }

            if (b == null) {
                b = new ActiveSession(r.nextInt(userCount) + offset, BURST_SESSION_BATCH_SIZE);
                shc.incCounter(SESSION_IS_NEW);
            }

        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        return b;
    }

    /**
     * Go an see how long it's been
     */
    private void gatherInputLagStats() {
        try {

            shc.init(INPUT_LAG, INPUT_LAG_HISTOGRAM_SIZE, "time from record creation to processing");

            // See if param is set. If it is, update table DDL...
            ClientResponse cr = voltClient.callProcedure("get_processing_lag");

            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable lagTable = cr.getResults()[0];
                while (lagTable.advanceRow()) {
                    int lagMs = (int) lagTable.getLong("lag_ms");
                    long howMany = lagTable.getLong("how_many");

                    if (lagMs < AbstractMediationProcedure.ONE_WEEK_IN_MS) {
                        shc.get(INPUT_LAG).pokeValue(lagMs, howMany);
                    }
                }
            }

        } catch (IOException | ProcCallException e1) {
            msg("Error:" + e1.getMessage());
        }

    }

    /**
     * Update the table cdr_dupcheck and change how long we keep records for. We do
     * this at the start of each run because users might want to play around with
     * the parameter DUPCHECK_TTLMINUTES between runs.
     */
    private void setDupcheckTTLMinutes() {
        try {

            // Default is one day
            long ttlMinutes = 1440;

            // See if param is set. If it is, update table DDL...
            ClientResponse cr = voltClient.callProcedure("@AdHoc",
                    "SELECT parameter_value FROM mediation_parameters WHERE parameter_name = 'DUPCHECK_TTLMINUTES';");

            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable paramTable = cr.getResults()[0];
                if (paramTable.advanceRow()) {
                    ttlMinutes = paramTable.getLong("parameter_value");
                    voltClient.callProcedure("@AdHoc", "alter table cdr_dupcheck alter USING TTL " + ttlMinutes
                            + " MINUTES ON COLUMN insert_date BATCH_SIZE 50000 MAX_FREQUENCY 200;");
                }
            }

        } catch (IOException | ProcCallException e1) {
            msg("Error:" + e1.getMessage());
        }
    }

    /**
     * Print general status info
     *
     * @param startMsReported
     */
    private void printStatus(long startMsReported) {

        msg("normalCDRCount = " + normalCDRCount);
        msg("missingCount = " + missingCount);
        msg("dupCount = " + dupCount);
        msg("lateCount = " + lateCount);
        msg("dateis1970Count = " + dateis1970Count);
        msg("useKafka = " + useKafka);

        if (System.currentTimeMillis() > 5000 + startMsReported) {
            long observedTps = (normalCDRCount + missingCount + dupCount + lateCount + dateis1970Count)
                    / ((System.currentTimeMillis() - startMsReported) / 1000);
            msg("observedTps = " + observedTps);
        }

        shc.incCounter("normalCDRCount", normalCDRCount);

    }

    /**
     * Send any messages in the late or duplicates queues. Note this is not rate
     * limited, and may cause a latency spike
     */
    private void sendRemainingMessages() {
        // Send late messages
        msg("sending " + lateMessages.size() + " late messages");

        while (lateMessages.size() > 0) {
            MediationMessage lateCDR = lateMessages.remove(0);
            send(lateCDR, null);
        }

        // Send dup messages
        msg("sending " + dupMessages.size() + " duplicate messages");
        while (dupMessages.size() > 0) {
            MediationMessage dupCDR = dupMessages.remove(0);
            send(dupCDR, null);
        }
    }

    /**
     * Send a CDR to VoltDB via Kafka.
     *
     * @param nextCdr
     */
    private void send(MediationMessage nextCdr, ActiveSession pseudoRandomSession) {

        if (useKafka) {

            final long startMs = System.currentTimeMillis();

            MediationCDRKafkaCallback coekc = new MediationCDRKafkaCallback(pseudoRandomSession, activeSessionQueue);
            ProducerRecord<Long, MediationMessage> newRecord = new ProducerRecord<>("incoming_cdrs",
                    nextCdr.getSessionId(), nextCdr);
            producer.send(newRecord, coekc);

            shc.report(MediationDataGenerator.INPUT_CLIENT_LAG, (int) (System.currentTimeMillis() - startMs), "", 1000);

        } else {
            sendViaVoltDB(nextCdr, pseudoRandomSession);
        }

    }

    /**
     * Send CDR directly to VoltDB, in case you want to see if there's a difference.
     *
     * @param nextCdr
     * @param pseudoRandomSession
     */
    private void sendViaVoltDB(MediationMessage nextCdr, ActiveSession pseudoRandomSession) {

        if (voltClient != null) {
            try {
                MediationCDRCallback coec = new MediationCDRCallback(pseudoRandomSession, activeSessionQueue);

                final long startMs = System.currentTimeMillis();

                voltClient.callProcedure(coec, "HandleMediationCDR", nextCdr.getSessionId(),
                        nextCdr.getSessionStartUTC(), nextCdr.getSeqno(), nextCdr.getCallingNumber(),
                        nextCdr.getDestination(), nextCdr.getEventType(), nextCdr.getRecordStartUTC(),
                        nextCdr.getRecordUsage());

                shc.report(MediationDataGenerator.INPUT_CLIENT_LAG, (int) (System.currentTimeMillis() - startMs), "",
                        1000);

            } catch (Exception e) {
                msg(e.getMessage());
            }
        }

    }

    /**
     * @return A random website.
     */
    private String getRandomDestinationId() {

        if (r.nextInt(10) == 0) {
            return "www.nytimes.com";
        }

        if (r.nextInt(10) == 0) {
            return "www.cnn.com";
        }

        return "www.voltdb.com";
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 13) {
            msg("Usage: MediationDataGenerator hostnames kafkaHostnames userCount tpMs durationSeconds missingRatio dupRatio lateRatio dateis1970Ratio offset userkafkaflag kafkaPort maxActiveSessions");
            msg("where missingRatio, dupRatio, lateRatio and dateis1970Ratio are '1 in' ratios - i.e. 100 means 1%");
            msg("For userkafkaflag 0 means speak directy to Volt. 1 means to use kafka instead. 2 means use Kafka to write but don't run a consumer");
            msg("maxActiveSessions is a special cache for 'busy' sessions");
            System.exit(2);
        }

        String hostnames = args[0];
        String kafkaHostnames = args[1];
        int userCount = Integer.parseInt(args[2]);
        int tpMs = Integer.parseInt(args[3]);
        int durationSeconds = Integer.parseInt(args[4]);
        int missingRatio = Integer.parseInt(args[5]);
        int dupRatio = Integer.parseInt(args[6]);
        int lateRatio = Integer.parseInt(args[7]);
        int dateis1970Ratio = Integer.parseInt(args[8]);
        int offset = Integer.parseInt(args[9]);
        int useKafka = Integer.parseInt(args[10]);
        int kafkaPort = Integer.parseInt(args[11]);
        int maxActiveSessions = Integer.parseInt(args[12]);

        MediationDataGenerator a = new MediationDataGenerator(hostnames, kafkaHostnames, userCount, durationSeconds,
                tpMs, durationSeconds, missingRatio, dupRatio, lateRatio, dateis1970Ratio, useKafka, kafkaPort,
                maxActiveSessions);

        boolean ok = a.run(offset);

        if (ok) {
            System.exit(0);
        }

        msg(UNABLE_TO_MEET_REQUESTED_TPS);
        System.exit(1);

    }

    /**
     *
     * Connect to VoltDB using native APIS
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);
            config.setHeavyweight(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

            if (client.getConnectedHostList().size() > 0) {
                msg("Connected to Volt");
            }
            else {
                msg("Warning: Not Connected to Volt");
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Volt connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Connect to VoltDB using Kafka APIS
     *
     * @param commaDelimitedHostnames
     * @param keySerializer
     * @param valueSerializer
     * @param kafkaPort
     * @return A Kafka Producer for MediationMessages
     * @throws Exception
     */
    private static Producer<Long, MediationMessage> connectToKafka(String commaDelimitedHostnames, String keySerializer,
            String valueSerializer, int kafkaPort) throws Exception {

        String[] hostnameArray = commaDelimitedHostnames.split(",");

        StringBuffer kafkaBrokers = new StringBuffer();
        for (int i = 0; i < hostnameArray.length; i++) {
            kafkaBrokers.append(hostnameArray[i]);
            kafkaBrokers.append(":");
            kafkaBrokers.append(kafkaPort);

            if (i < (hostnameArray.length - 1)) {
                kafkaBrokers.append(',');
            }
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers.toString());
        props.put("acks", "1");
        props.put("retries", 0);

        // batch.size is how many bytes a batch can take up, not how many records in a
        // batch
        props.put("batch.size", 1638400);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        // Allow user to override partitioner with -DKAFKA_PARTITONER_NAME=foo
        String partitionerName = System.getProperty(KAFKA_PARTITONER_NAME, VoltDBKafkaPartitioner.class.getName());

        if (partitionerName.length() > 0) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerName);
        } else {
            msg("not setting ProducerConfig.PARTITIONER_CLASS_CONFIG");
        }

        msg("Connecting to Kafka using " + kafkaBrokers.toString() + " and " + partitionerName);

        Producer<Long, MediationMessage> newProducer = new KafkaProducer<>(props);

        msg("Connected to Kafka");

        return newProducer;

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Check VoltDB to see how things are going...
     *
     * @param client
     * @param nextCdr
     */
    public static void printApplicationStats(Client client, MediationMessage nextCdr) {
        try {

            msg("");
            msg("Latest Stats:");
            msg("");

            ClientResponse cr = client.callProcedure("ShowAggStatus__promBL");
            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable[] resultsTables = cr.getResults();
                for (VoltTable resultsTable : resultsTables) {
                    if (resultsTable.advanceRow()) {
                        msg(resultsTable.toFormattedString());
                    }

                }

            }

            cr = client.callProcedure("GetBySessionId", nextCdr.getSessionId(), new Date(nextCdr.getSessionStartUTC()));

            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable[] resultsTables = cr.getResults();
                for (VoltTable resultsTable : resultsTables) {
                    if (resultsTable.advanceRow()) {
                        msg(resultsTable.toFormattedString());
                    }

                }

            }

        } catch (IOException | ProcCallException e1) {
            msg("Error:" + e1.getMessage());
        }

    }

    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

        oneLineSummary.append(tpMs);
        oneLineSummary.append(':');

        oneLineSummary.append(tps);
        oneLineSummary.append(':');

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, INPUT_LAG);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, OUTPUT_LAG);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, OUTPUT_POLL_BATCHSIZE);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, INPUT_CLIENT_LAG);

        msg(oneLineSummary.toString());

        msg(shc.toString());
    }

}

package org.voltdb.aggdemo;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2023 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.voltdb.voltutil.stats.SafeHistogramCache;

/**
 * Runnable class to receive policy change messages and update our collection of
 * virtual sessions.
 *
 */
public class AggregatedRecordConsumer implements Runnable {

    /**
     * Handle for stats
     */
    SafeHistogramCache sph = SafeHistogramCache.getInstance();

    /**
     * Comma delimited list of Kafka hosts. Note we expect the port number with each
     * host name
     */
    String commaDelimitedHostnames;

    /**
     * Keep running until told to stop..
     */
    boolean keepGoing = true;

    /**
     * Date format mask
     */
    final String VOLT_EXPORTED_DATE_MASK = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * Date formatter for input data
     */
    SimpleDateFormat formatter = new SimpleDateFormat(VOLT_EXPORTED_DATE_MASK);

    int kafkaPort;

    /**
     * Create a runnable instance of a class to poll the Kafka topic aggregated_cdrs
     *
     * @param hostnames - hostname1:9092,hostname2:9092 etc
     */
    public AggregatedRecordConsumer(String commaDelimitedHostnames, int kafkaPort) {
        super();
        this.commaDelimitedHostnames = commaDelimitedHostnames;
        this.kafkaPort = kafkaPort;
    }

    @Override
    public void run() {

        try {

            MediationDataGenerator.msg("AggregatedRecordConsumer starting...");
            
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
            props.put("bootstrap.servers", kafkaBrokers.toString() + ",");
            props.put("group.id", "AggregatedRecordConsumer" + System.currentTimeMillis());
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.commit.interval.ms", "100");
            props.put("auto.offset.reset", "latest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("aggregated_cdrs"));

            long messageCounter = 0;

            while (keepGoing) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                sph.incCounter("CONSUMER_POLLS");

                Date aggDate = null;

                if (records != null) {
                    
                    sph.reportSize("POLL_SIZE", kafkaPort, "", 10000);
                    
                    sph.incCounter("AGGED_RECORDS", records.count());

                    for (ConsumerRecord<String, String> record : records) {

                        messageCounter++;

                            String commaSeperatedValue = record.value();
                            MediationDataGenerator.msg(commaSeperatedValue);
                            
                            String[] commaSeperatedValues = commaSeperatedValue.split(",");

                            Date tempDate = formatter.parse(commaSeperatedValues[10]);

                            // Find oldest record in the batch...
                            if (aggDate == null || tempDate.before(aggDate)) {
                                aggDate = new Date(tempDate.getTime());
                            }

                        

                    }

                    if (records.count() > 0) {
                        sph.reportLatency(MediationDataGenerator.OUTPUT_LAG, aggDate.getTime(), "",
                                MediationDataGenerator.OUTPUT_LAG_HISTOGRAM_SIZE, records.count());

                        sph.report(MediationDataGenerator.OUTPUT_POLL_BATCHSIZE, records.count(), "", 1000);
                    }
                }

            }

            consumer.close();
            MediationDataGenerator.msg("AggregatedRecordConsumer halted");

        } catch (Exception e1) {
            MediationDataGenerator.msg("AggregatedRecordConsumer: " + e1.getMessage());
        }

    }

    /**
     * Stop polling for messages and exit.
     */
    public void stop() {
        MediationDataGenerator.msg("Halting");
        keepGoing = false;
    }

    public static void main(String[] args) throws Exception {
        
        MediationDataGenerator.msg("Parameters:" + Arrays.toString(args));

        if (args.length != 3) {
            MediationDataGenerator.msg("Usage: AggregatedRecordConsumer kafkaHostnames kafkaPort durationSeconds");
            System.exit(2);
        }
        
        String kafkaHostnames = args[0];
        int kafkaPort = Integer.parseInt(args[1]);
        int durationSeconds = Integer.parseInt(args[2]);

        AggregatedRecordConsumer arc = new AggregatedRecordConsumer(kafkaHostnames, kafkaPort);

        Thread thread = new Thread(arc, "AggRecordConsumer");
        thread.start();
        
        MediationDataGenerator.msg("Agg record consumer is thread " + thread.getName());

        Thread.sleep(durationSeconds * 1000);

        arc.stop();
        
        SafeHistogramCache sph = SafeHistogramCache.getInstance();
        
        MediationDataGenerator.msg(sph.toString());
        System.exit(0);

    }

}

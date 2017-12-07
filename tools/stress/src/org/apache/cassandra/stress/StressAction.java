/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.cassandra.client.ClientContext;
import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.Cassandra;

public class StressAction extends Thread {
    /**
     * Producer-Consumer model: 1 producer, N consumers
     */
    private final BlockingQueue<Operation> operations = new SynchronousQueue<Operation>(true);

    private final Session client;
    private final PrintStream output;
    private final ClientContext clientContext;

    private volatile boolean stop = false;
    public StressAction(Session session, PrintStream out, ClientContext clientContext) {
        client = session;
        output = out;
        this.clientContext = clientContext;
    }

    @Override
    public void run() {
        long latency, oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;
        int columnCount, oldColumnCount;
        long byteCount, oldByteCount;

        // creating keyspace and column families
        if (client.getOperation() == Stress.Operations.INSERT || client.getOperation() == Stress.Operations.COUNTER_ADD || client.getOperation() == Stress.Operations.INSERTCL || client.getOperation() == Stress.Operations.FACEBOOK_POPULATE)
            client.createKeySpaces();

        int threadCount = client.getThreads();
        Consumer[] consumers = new Consumer[threadCount];



        int itemsPerThread = client.getKeysPerThread();
        int modulo = client.getNumKeys() % threadCount;

        // creating required type of the threads for the test
        for (int i = 0; i < threadCount; i++) {
            if (i == threadCount - 1)
                itemsPerThread += modulo; // last one is going to handle N + modulo items

            consumers[i] = new Consumer(itemsPerThread);
        }

        Producer producer = new Producer();

        // Wait until all clients are up
        if(client.getOperation() == Stress.Operations.EXP10) {
            try {
                new ClientSyncer(client, -1, output).run(client.getClientLibrary());
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }
        }
        producer.start();

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // initialization of the values
        boolean terminate = false;
        latency = byteCount = 0;
        epoch = total = keyCount = columnCount = 0;
        output.println("total,interval_op_rate,interval_key_rate,avg_latency,elapsed_time");
        int interval = client.getProgressInterval();
        int epochIntervals = client.getProgressInterval() * 10;
        long testStartTime = System.currentTimeMillis();

        while (!terminate) {
            if (stop || client.exptDurationMs > client.specifiedExptDurationSeconds * 1000)
            {
                producer.stopProducer();

                for (Consumer consumer : consumers)
                    consumer.stopConsume();

                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals) {
                epoch = 0;

                oldTotal = total;
                oldLatency = latency;
                oldKeyCount = keyCount;
                oldColumnCount = columnCount;
                oldByteCount = byteCount;

                total = client.operations.get();
                keyCount = client.keys.get();
                columnCount = client.columnCount.get();
                byteCount = client.bytes.get();
                latency = client.latency.get();

                int opDelta = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;
                int columnDelta = columnCount - oldColumnCount;
                long byteDelta = byteCount - oldByteCount;
                double latencyDelta = latency - oldLatency;

                client.exptDurationMs = System.currentTimeMillis() - testStartTime;
                long currentTimeInSeconds =  client.exptDurationMs / 1000;
                String formattedDelta = (opDelta > 0) ? Double.toString(latencyDelta / (opDelta * 1000)) : "NaN";

                output.println(String.format("%d,%d,%d,%d,%d,%s,%d", total, opDelta / interval, keyDelta / interval, columnDelta / interval, byteDelta / interval, formattedDelta, currentTimeInSeconds));
            }
        }
        printLatencyPercentiles();
        // marking an end of the output to the client
        output.println("END");
    }
    private Long mean(Long[] array) {
        long sum = 0;
        for(int i = 0; i < array.length; ++i)
            sum += array[i];
        return sum/array.length;
    }
    private Long percentile(Long[] array, double percentile) {
        return array[(int) (array.length * (percentile / 100))];

    }

    private void printLatencyPercentiles() {


        // Trim away the latencies from the start and end of the trial
        // we'll go with 1/4 from each end, as in COPS we did 15 secs off each side of 60
        ArrayDeque<Long> latenciesDeque = new ArrayDeque<Long>();
        latenciesDeque.addAll(client.latencies);
        int trimLen = latenciesDeque.size() / 4;
        for (int ii = 0; ii < trimLen; ii++) {
            latenciesDeque.removeFirst();
            latenciesDeque.removeLast();
        }

        // Sort the latencies so we can find percentiles
        SortedSet<Long> latenciesSet = new TreeSet<Long>();
        latenciesSet.addAll(latenciesDeque);
        Long[] latencies = latenciesSet.toArray(new Long[0]);

        if (latencies.length == 0) {
            // We aren't recording latencies for this op type probably
//            System.err.println("No Latencies percentiles to print");
            return;
        }

//        System.err.println(String.format("Overall Latencies (usecs): 50=%d, 90=%d, 95=%d, 99=%d, 99.9=%d",
//                percentile(latencies, 50), percentile(latencies, 90), percentile(latencies, 95),
//                percentile(latencies, 99), percentile(latencies, 99.9)));


        // RO6 read latency
        ArrayDeque<Long> readlatenciesDeque = new ArrayDeque<Long>();
        readlatenciesDeque.addAll(client.readlatencies);
        int readtrimLen = readlatenciesDeque.size() / 4;
        for (int ii = 0; ii < readtrimLen; ii++) {
            readlatenciesDeque.removeFirst();
            readlatenciesDeque.removeLast();
        }

        // Sort the latencies so we can find percentiles
        SortedSet<Long> readlatenciesSet = new TreeSet<Long>();
        readlatenciesSet.addAll(readlatenciesDeque);
        Long[] readlatencies = readlatenciesSet.toArray(new Long[0]);

        if (readlatencies.length == 0) {
            // We aren't recording latencies for this op type probably
//            System.err.println("No ReadLatencies percentiles to print");
            return;
        }

//        System.err.println(String.format("Read Latencies (usecs): 50=%d, 90=%d, 95=%d, 99=%d, 99.9=%d",
//                percentile(readlatencies, 50), percentile(readlatencies, 90), percentile(readlatencies, 95),
//                percentile(readlatencies, 99), percentile(readlatencies, 99.9)));

        // @Khiem: print out all read latencies
        //System.err.println(String.format("List of Read Latencies: %s", Arrays.toString(readlatencies)));


        //RO6 write latency
        ArrayDeque<Long> writelatenciesDeque = new ArrayDeque<Long>();
        writelatenciesDeque.addAll(client.writelatencies);
        int writetrimLen = writelatenciesDeque.size() / 4;
        for (int ii = 0; ii < writetrimLen; ii++) {
            writelatenciesDeque.removeFirst();
            writelatenciesDeque.removeLast();
        }

        // Sort the latencies so we can find percentiles
        SortedSet<Long> writelatenciesSet = new TreeSet<Long>();
        writelatenciesSet.addAll(writelatenciesDeque);
        Long[] writelatencies = writelatenciesSet.toArray(new Long[0]);

        if (writelatencies.length == 0) {
            // We aren't recording latencies for this op type probably
//            System.err.println("No WriteLatencies percentiles to print");
            return;
        }

//        System.err.println(String.format("Write Latencies (usecs): 50=%d, 90=%d, 95=%d, 99=%d, 99.9=%d",
//                percentile(writelatencies, 50), percentile(writelatencies, 90), percentile(writelatencies, 95),
//                percentile(writelatencies, 99), percentile(writelatencies, 99.9)));

        /*
        Expt,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,Threads,Client,NumOps,NumKeys,NumColumns,NumBytes,NUmReads,NumWrites,Duration,Throughput,Ravg,R50,R90,R99,Wavg,W50,W90,W99,#Tx2R,#K2R,#R,#W
         */

        int numReads = client.readlatencies.size();
        int numWrites = client.writelatencies.size();
        int numOps = client.operations.get();
        int numKeys = client.keys.get();
        int numColumns = client.columnCount.get();
        long numBytes = client.bytes.get();
        long duration = client.exptDurationMs;
        int num2RoundTxn =client.numRound2Txns.get();
        int num2RoundKey =client.numRound2Keys.get();
        int nR = client.numReads.get();
        int nW = client.numWrites.get();

        //Expt,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,Threads,Client
        ArrayList<String> outputs = new ArrayList<>();
        outputs.add("Eiger");
        outputs.add(String.valueOf(client.getKeys_per_server()));
        outputs.add(String.valueOf(client.getNum_servers()));
        outputs.add(String.valueOf(client.getColumnSize()));
        outputs.add(String.valueOf(client.getKeys_per_read()));
        outputs.add(String.valueOf(client.getWrite_fraction()));
        outputs.add(String.valueOf(client.getZipfianConstant()));
        outputs.add(String.valueOf(client.getThreads()));
        outputs.add("Client"+client.stressIndex);

        //NumOps,NumKeys,NumColumns,NumBytes,NUmReads,NumWrites,Duration,Throughput
        outputs.add(String.valueOf(numOps));
        outputs.add(String.valueOf(numKeys));
        outputs.add(String.valueOf(numColumns));
        outputs.add(String.valueOf(numBytes));
        outputs.add(String.valueOf(numReads));
        outputs.add(String.valueOf(numWrites));
        outputs.add(String.valueOf(duration));
        outputs.add(String.valueOf(numOps*1000/duration));

        //Ravg,R50,R90,R99
        outputs.add(String.valueOf(mean(readlatencies)));
        outputs.add(String.valueOf(percentile(readlatencies,50)));
        outputs.add(String.valueOf(percentile(readlatencies,90)));
        outputs.add(String.valueOf(percentile(readlatencies,99)));

        //Wavg,W50,W90,W99
        outputs.add(String.valueOf(mean(writelatencies)));
        outputs.add(String.valueOf(percentile(writelatencies,50)));
        outputs.add(String.valueOf(percentile(writelatencies,90)));
        outputs.add(String.valueOf(percentile(writelatencies,99)));

        //#Tx2R,#K2R, #R,#W
        outputs.add(String.valueOf(num2RoundTxn));
        outputs.add(String.valueOf(num2RoundKey));
        outputs.add(String.valueOf(nR));
        outputs.add(String.valueOf(nW));

        System.err.println(String.join(",",outputs));

    }

    /**
     * Produces exactly N items (awaits each to be consumed)
     */
    private class Producer extends Thread {
        private volatile boolean stop = false;

        @Override
        public void run() {
            for (int i = 0; i < client.getNumKeys(); i++) {
                if (stop)
                    break;

                try {
                    operations.put(createOperation((i % client.getNumDifferentKeys()) + client.getKeysOffset()));
                } catch (InterruptedException e) {
                    System.err.println("Producer error - " + e.getMessage());
                    return;
                }
            }
        }

        public void stopProducer() {
            stop = true;
        }
    }

    /**
     * Each consumes exactly N items from queue
     */
    private class Consumer extends Thread {
        private final int items;
        private volatile boolean stop = false;

        public Consumer(int toConsume) {
            items = toConsume;
        }

        @Override
        public void run() {
            if (client.getOperation() == Stress.Operations.DYNAMIC ||
                    client.getOperation() == Stress.Operations.INSERTCL ||
                    client.getOperation() == Stress.Operations.FACEBOOK ||
                    client.getOperation() == Stress.Operations.FACEBOOK_POPULATE ||
                    client.getOperation() == Stress.Operations.WRITE_TXN ||
                    client.getOperation() == Stress.Operations.BATCH_MUTATE ||
                    client.getOperation() == Stress.Operations.TWO_ROUND_READ_TXN ||
                    client.getOperation() == Stress.Operations.DYNAMIC_ONE_SERVER ||
                    client.getOperation() == Stress.Operations.EXP10) {
                ClientLibrary library = client.getClientLibrary();

                for (int i = 0; i < items; i++) {
                    if (stop)
                        break;

                    try {
                        operations.take().run(library); // running job
                        client.numRound2Txns.addAndGet(library.numTwoRoundTxns);
                        client.numRound2Keys.addAndGet(library.numTwoRoundKeys);
                        library.numTwoRoundTxns = 0;
                        library.numTwoRoundKeys = 0;
                    } catch (Exception e) {
                        if (output == null) {
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                            System.exit(-1);
                        }
                        output.println(e.getMessage());
                        e.printStackTrace();
                        break;
                    }
                }
            } else {
                Cassandra.Client connection = client.getClient();

                for (int i = 0; i < items; i++) {
                    if (stop)
                        break;

                    try {
                        operations.take().run(connection); // running job
                    } catch (Exception e) {
                        if (output == null) {
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                            System.exit(-1);
                        }


                        output.println(e.getMessage());
                        e.printStackTrace();
                        break;
                    }
                }
            }
        }

        public void stopConsume() {
            stop = true;
        }
    }

    private Operation createOperation(int index) {
        switch (client.getOperation()) {
            case READ:
                return client.isCQL() ? new CqlReader(client, index) : new Reader(client, index, clientContext);

            case COUNTER_GET:
                return client.isCQL() ? new CqlCounterGetter(client, index) : new CounterGetter(client, index, clientContext);

            case INSERT:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case COUNTER_ADD:
                return client.isCQL() ? new CqlCounterAdder(client, index) : new CounterAdder(client, index, clientContext);

            case RANGE_SLICE:
                return client.isCQL() ? new CqlRangeSlicer(client, index) : new RangeSlicer(client, index, clientContext);

            case INDEXED_RANGE_SLICE:
                return client.isCQL() ? new CqlIndexedRangeSlicer(client, index) : new IndexedRangeSlicer(client, index, clientContext);

            case MULTI_GET:
                return client.isCQL() ? new CqlMultiGetter(client, index) : new MultiGetter(client, index, clientContext);

            case DYNAMIC:
                if (client.isCQL())
                    throw new RuntimeException("CQL not supprted with dynamic workload");
                return new DynamicWorkload(client, index);

            case DYNAMIC_ONE_SERVER:
                if (client.isCQL())
                    throw new RuntimeException("CQL not supprted with dynamic workload");
                return new DynamicOneServer(client, index);

            case INSERTCL:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case EXP10:
                if (client.isCQL())
                    throw new RuntimeException("CQL is not supported with Exp10 workload");
                return new Experiment10(client, index);

            case WRITE_TXN:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with write txn workload");
                return new WriteTransactionWorkload(client, index, true);

            case BATCH_MUTATE:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with write txn workload");
                return new WriteTransactionWorkload(client, index, false);

            case TWO_ROUND_READ_TXN:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
                return new TwoRoundReadTxn(client, index);

            case FACEBOOK_POPULATE:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
                return new FacebookPopulator(client, index);

            case FACEBOOK:
                if (client.isCQL())
                    throw new RuntimeException("CQL not support with this workload");
                return new FacebookWorkload(client, index);
        }

        throw new UnsupportedOperationException();
    }

    public void stopAction() {
        stop = true;
    }
}

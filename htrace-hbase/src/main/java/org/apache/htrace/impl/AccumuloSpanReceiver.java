/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.htrace.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.htrace.HBaseHTraceConfiguration;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.protobuf.generated.SpanProtos;

/**
 * HBase is an open source distributed datastore. This span receiver store spans
 * into HBase. HTrace spans are queued into a blocking queue. From there
 * background worker threads will send them to a HBase database.
 */
public class AccumuloSpanReceiver implements SpanReceiver {
    private static final Log LOG = LogFactory.getLog(AccumuloSpanReceiver.class);

    public static final String COLLECTOR_QUORUM_KEY = "htrace.hbase.collector-quorum";
    public static final String DEFAULT_COLLECTOR_QUORUM = "127.0.0.1";
    public static final String ZOOKEEPER_CLIENT_PORT_KEY = "htrace.hbase.zookeeper.property.clientPort";
    public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;
    public static final String ZOOKEEPER_ZNODE_PARENT_KEY = "htrace.hbase.zookeeper.znode.parent";
    public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase";
    public static final String NUM_THREADS_KEY = "htrace.hbase.num-threads";
    public static final int DEFAULT_NUM_THREADS = 1;
    public static final String MAX_SPAN_BATCH_SIZE_KEY = "htrace.hbase.batch.size";
    public static final int DEFAULT_MAX_SPAN_BATCH_SIZE = 100;
    public static final String TABLE_KEY = "htrace.hbase.table";
    public static final String DEFAULT_TABLE = "htrace";
    public static final String COLUMNFAMILY_KEY = "htrace.hbase.columnfamily";
    public static final String DEFAULT_COLUMNFAMILY = "s";
    public static final String INDEXFAMILY_KEY = "htrace.hbase.indexfamily";
    public static final String DEFAULT_INDEXFAMILY = "i";
    public static final byte[] INDEX_SPAN_QUAL = Bytes.toBytes("s");
    public static final byte[] INDEX_TIME_QUAL = Bytes.toBytes("t");

    /**
     * How long this receiver will try and wait for all threads to shutdown.
     */
    private static final int SHUTDOWN_TIMEOUT = 30;

    /**
     * How many errors in a row before we start dropping traces on the floor.
     */
    private static final int MAX_ERRORS = 10;

    /**
     * The queue that will get all HTrace spans that are to be sent.
     */
    private final BlockingQueue<Span> queue;

    /**
     * Boolean used to signal that the threads should end.
     */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * The thread factory used to create new ExecutorService.
     * <p/>
     * This will be the same factory for the lifetime of this object so that no
     * thread names will ever be duplicated.
     */
    private final ThreadFactory tf = new ThreadFactory() {
        private final AtomicLong receiverIdx = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(String.format("accumuloSpanReceiver-%d", receiverIdx.getAndIncrement()));
            return t;
        }
    };

    private ExecutorService service;
    private final Configuration hconf;
    private final int maxSpanBatchSize;
    private final ProcessId processId;

    public AccumuloSpanReceiver(HTraceConfiguration conf) {
        this.queue = new ArrayBlockingQueue<Span>(1000);
        this.hconf = HBaseConfiguration.create();
        this.maxSpanBatchSize = conf.getInt(MAX_SPAN_BATCH_SIZE_KEY, DEFAULT_MAX_SPAN_BATCH_SIZE);
        String quorum = conf.get(COLLECTOR_QUORUM_KEY, DEFAULT_COLLECTOR_QUORUM);
        hconf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
        String znodeParent = conf.get(ZOOKEEPER_ZNODE_PARENT_KEY, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        hconf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);
        int clientPort = conf.getInt(ZOOKEEPER_CLIENT_PORT_KEY, DEFAULT_ZOOKEEPER_CLIENT_PORT);
        hconf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, clientPort);

        // If there are already threads runnnig tear them down.
        if (this.service != null) {
            this.service.shutdownNow();
            this.service = null;
        }
        int numThreads = conf.getInt(NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
        this.service = Executors.newFixedThreadPool(numThreads, tf);
        for (int i = 0; i < numThreads; i++) {
            this.service.submit(new WriteSpanRunnable());
        }
        this.processId = new ProcessId(conf);
    }

    private class WriteSpanRunnable implements Runnable {
        private Connector connector;
        private BatchWriter writer;

        public WriteSpanRunnable() {
        }

        /**
         * This runnable sends a HTrace span to the HBase.
         */
        @Override
        public void run() {
            SpanProtos.Span.Builder sbuilder = SpanProtos.Span.newBuilder();
            SpanProtos.TimelineAnnotation.Builder tlbuilder = SpanProtos.TimelineAnnotation.newBuilder();
            List<Span> dequeuedSpans = new ArrayList<Span>(maxSpanBatchSize);
            long errorCount = 0;

            while (running.get() || queue.size() > 0) {
                Span firstSpan = null;
                try {
                    // Block for up to a second. to try and get a span.
                    // We only block for a little bit in order to notice
                    // if the running value has changed
                    firstSpan = queue.poll(1, TimeUnit.SECONDS);

                    // If the poll was successful then it's possible that there
                    // will be other spans to get. Try and get them.
                    if (firstSpan != null) {
                        // Add the first one that we got
                        dequeuedSpans.add(firstSpan);
                        // Try and get up to 100 queues
                        queue.drainTo(dequeuedSpans, maxSpanBatchSize - 1);
                    }
                } catch (InterruptedException ie) {
                    // Ignored.
                }
                startClient();
                if (dequeuedSpans.isEmpty()) {
                    try {
                        writer.flush();
                    } catch (MutationsRejectedException e) {
                        LOG.error("failed to flush writes to Accumulo.");
                        e.printStackTrace();
                        closeClient();
                    }
                    continue;
                }

                try {
                    for (Span span : dequeuedSpans) {
                        sbuilder.clear().setTraceId(span.getTraceId()).setStart(span.getStartTimeMillis()).setStop(span.getStopTimeMillis())
                                .setSpanId(span.getSpanId()).setProcessId(span.getProcessId()).setDescription(span.getDescription());

                        if (span.getParents().length == 0) {
                            sbuilder.setParentId(0);
                        } else if (span.getParents().length > 0) {
                            sbuilder.setParentId(span.getParents()[0]);
                            if (span.getParents().length > 1) {
                                LOG.error("error: HBaseSpanReceiver does not support spans "
                                        + "with multiple parents.  Ignoring multiple parents for " + span);
                            }
                        }

                        for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
                            sbuilder.addTimeline(tlbuilder.clear().setTime(ta.getTime()).setMessage(ta.getMessage()).build());
                        }

                        String idString = Long.toHexString(span.getTraceId());
                        byte[] spanValue = sbuilder.build().toByteArray();

                        Mutation spanMutation = new Mutation(idString);
                        spanMutation.put(new Text("s"), new Text(spanValue), new Value());
                        writer.addMutation(spanMutation);

                        // create indexes if top-level span
                        if (span.getParents().length == 0) {
                            // create index for startTime
                            byte[] spanStart = Bytes.toBytes(span.getStartTimeMillis());
                            Mutation indexTimeMutation = new Mutation(idString);
                            indexTimeMutation.put(new Text("i"), new Text("t"), new Value(spanStart, 0, spanStart.length));
                            writer.addMutation(indexTimeMutation);

                            // create index for span
                            Mutation indexSpanMutation = new Mutation(idString);
                            indexSpanMutation.put(new Text("i"), new Text("s"), new Value(spanValue, 0, spanValue.length));
                            writer.addMutation(indexSpanMutation);
                        }

                    }
                    // clear the list for the next time through.
                    dequeuedSpans.clear();
                    // reset the error counter.
                    errorCount = 0;
                } catch (Exception e) {
                    errorCount += 1;
                    // If there have been ten errors in a row start dropping
                    // things.
                    if (errorCount < MAX_ERRORS) {
                        try {
                            queue.addAll(dequeuedSpans);
                        } catch (IllegalStateException ex) {
                            LOG.error("Drop " + dequeuedSpans.size() + " span(s) because writing to HBase failed.");
                        }
                    }
                    else
                    {
                        LOG.error("Reached maximum tries to write to Accumulo due to exception. " + e.getMessage());
                        e.printStackTrace();
                    }
                    
                    closeClient();
                    try {
                        // Since there was an error sleep just a little bit to
                        // try and allow the
                        // HBase some time to recover.
                        Thread.sleep(500);
                    } catch (InterruptedException e1) {
                        // Ignored
                    }
                }
            }
            closeClient();
        }

        /**
         * Close out the connection.
         */
        private void closeClient() {
            // close out the writer.
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    LOG.warn("Failed to close Accumulo batch writer. " + e.getMessage());
                    e.printStackTrace();
                }
                writer = null;
            }

        }

        /**
         * Re-connect to Accumulo
         */
        private void startClient() {
            if (writer == null) {
                try {
                    String instanceName = "training";
                    String zooServers = "localhost";
                    Instance inst = new ZooKeeperInstance(instanceName, zooServers);

                    connector = inst.getConnector("root", new PasswordToken("secret"));

                    // BatchWriterConfig has reasonable defaults
                    BatchWriterConfig config = new BatchWriterConfig();
                    config.setMaxMemory(10000000L); // bytes available to
                                                    // batchwriter for buffering
                                                    // mutations
                    writer = connector.createBatchWriter("htrace", config);
                } catch (AccumuloException e) {
                    LOG.warn("Failed to create Accumulo connection. " + e.getMessage());
                } catch (AccumuloSecurityException e) {
                    LOG.warn("Failed to create Acummulo connection. " + e.getMessage());
                } catch (TableNotFoundException e) {
                    LOG.warn("Failed to create connection to trace table. " + e.getMessage());
                }
            }
        }
    }

    /**
     * Close the receiver.
     * <p/>
     * This tries to shutdown thread pool.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        running.set(false);
        service.shutdown();
        try {
            if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.error("Was not able to process all remaining spans upon closing in: " + SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS
                        + ". Left Spans could be dropped.");
            }
        } catch (InterruptedException e1) {
            LOG.warn("Thread interrupted when terminating executor.", e1);
        }
    }

    @Override
    public void receiveSpan(Span span) {
        if (running.get()) {
            try {
                if (span.getProcessId().isEmpty()) {
                    span.setProcessId(processId.get());
                }
                queue.add(span);
            } catch (IllegalStateException e) {
                // todo: supress repeating error logs.
                LOG.error("Error trying to append span (" + span.getDescription() + ") to the queue. Blocking Queue was full.");
            }
        }
    }

    /**
     * Run basic test. Adds span to an existing htrace table in an existing
     * hbase setup. Requires a running accumulo to send the traces too with an
     * already created trace table (Default table name is 'htrace' with column
     * families 's' and 'i').
     * 
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        SpanReceiverBuilder builder = new SpanReceiverBuilder(new HBaseHTraceConfiguration(HBaseConfiguration.create()));
        SpanReceiver receiver = builder.spanReceiverClass(AccumuloSpanReceiver.class.getName()).build();
        Trace.addReceiver(receiver);
        TraceScope parent = Trace.startSpan("AccumuloSpanReceiver.main.parent", Sampler.ALWAYS);
        Thread.sleep(10);
        long traceid = parent.getSpan().getTraceId();
        TraceScope child1 = Trace.startSpan("AccumuloSpanReceiver.main.child.1");
        Thread.sleep(10);
        child1.close();
        TraceScope child2 = Trace.startSpan("AccumuloSpanReceiver.main.child.2", parent.getSpan());
        Thread.sleep(10);
        TraceScope gchild = Trace.startSpan("AccumuloSpanReceiver.main.grandchild");
        Trace.addTimelineAnnotation("annotation 1.");
        Thread.sleep(10);
        Trace.addTimelineAnnotation("annotation 2.");
        gchild.close();
        Thread.sleep(10);
        child2.close();
        Thread.sleep(10);

        parent.close();
        receiver.close();
        System.out.println("trace id: " + traceid);
    }
}

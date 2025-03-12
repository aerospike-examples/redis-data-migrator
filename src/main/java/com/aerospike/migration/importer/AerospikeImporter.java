package com.aerospike.migration.importer;

import java.awt.Desktop.Action;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.Eof;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import net.whitbeck.rdbparser.SelectDb;

public class AerospikeImporter {
    private final MappingSpecs specs;
    private final IAerospikeClient client;
    private final ArrayBlockingQueue<Entry> queue;
    private final AerospikeImporterOptions options;
    private final int threadsToUse;
    private Thread producer;
    private Thread monitor;
    private File errorFile = null;
    private PrintWriter errorWriter = null;
    private volatile boolean done = false;
    private final AtomicLong success = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final ExecutorService executor;
    
    public AerospikeImporter(AerospikeImporterOptions options) throws Exception {
        this.options = options;
        this.specs = options.getMappingSpecs();
        this.threadsToUse = options.getThreads() <= 0 ? Runtime.getRuntime().availableProcessors() : options.getThreads();
        this.client = this.connect();
        if (options.getErrorFileName() != null) {
            this.errorFile = new File(options.getErrorFileName());
            this.errorWriter = new PrintWriter(new BufferedWriter(new FileWriter(this.errorFile)));
        }
        this.queue = new ArrayBlockingQueue<>(options.getMaxQueueDepth());
        this.executor = Executors.newFixedThreadPool(threadsToUse);
    }
    
    private IAerospikeClient connect() {
        ClientPolicy clientPolicy = new ClientPolicy();
        ClusterConfig config = this.options.getCluster();
        
        clientPolicy.user = config.getUserName();
        clientPolicy.password = config.getPassword();
        clientPolicy.tlsPolicy = config.getTls() == null ? null : config.getTls().toTlsPolicy();
        clientPolicy.authMode = config.getAuthMode();
        clientPolicy.clusterName = config.getClusterName();
        clientPolicy.useServicesAlternate = config.isUseServicesAlternate();
        clientPolicy.minConnsPerNode = this.threadsToUse;

        WritePolicy defaultWritePolicy = new WritePolicy();
        defaultWritePolicy.recordExistsAction = this.options.getRecordExistsAction();
        defaultWritePolicy.sendKey = this.options.isSendKey();
        clientPolicy.writePolicyDefault = defaultWritePolicy;

        Host[] hosts = Host.parseHosts(this.options.getHost(), 3000);
        
        if (clientPolicy.user != null && clientPolicy.password == null) {
            java.io.Console console = System.console();
            if (console != null) {
                char[] pass = console.readPassword("Enter password for cluster: ");
                if (pass != null) {
                    clientPolicy.password = new String(pass);
                }
            }
        }
        return new AerospikeClient(clientPolicy, hosts);
    }
    
    private void logError(String format, Object ... args) {
        String value = String.format(format, args);
        if (!options.isSilent()) {
            System.out.println(value);
        }
        if (errorWriter != null) {
            errorWriter.println(value);
            errorWriter.flush();
        }
    }
    
    private void logError(Exception e) {
        this.logError("Exception %s: %s", e.getClass().getSimpleName(), e.getMessage());
    }
    
    public void start() {
        this.producer = new Thread(()-> {
            try {
                parseRdbFile(new File(options.getInputFileName()));
                this.done = true;
            }
            catch (Exception e) {
                logError(e);
                this.done = true;
            }
        }, "producer");
        this.producer.setDaemon(true);
        this.producer.start();
        
        for (int i = 0; i < this.threadsToUse; i++) {
            executor.execute(() -> {
                this.activeThreads.incrementAndGet();
                
                try {
                    while (!done || !queue.isEmpty()) {
                        try {
                            Entry item = queue.poll(1, TimeUnit.SECONDS);
                            if (item != null) {
                                processRecord(item);
                            }
                            success.incrementAndGet();
                        } catch (InterruptedException ignored) {
                        } catch (Exception ex) {
                            failed.incrementAndGet();
                            logError(ex);
                        }
                    }
                }
                finally {
                    this.activeThreads.decrementAndGet();
                }
            });
        }
        executor.shutdown();
    }
    
    private void processRecord(Entry e) throws Exception {
        switch (e.getType()) {

        case SELECT_DB:
            if (options.isVerbose()) {
                System.out.println("Processing DB: " + ((SelectDb)e).getId());
                System.out.println("------------");
            }
            break;

        case EOF:
            if (options.isVerbose()) {
                System.out.print("End of file. Checksum: ");
                for (byte b : ((Eof)e).getChecksum()) {
                    System.out.print(String.format("%02x", b & 0xff));
                }
                System.out.println();
                System.out.println("------------");
            }
            break;

        case KEY_VALUE_PAIR:
            KeyValuePair kvp = (KeyValuePair)e;
            String key = new String(kvp.getKey(), "ASCII");
            RecordTranslator translator = specs.getTranslatorFromString(key);

            if (options.isVerbose()) {
                System.out.println("Key value pair");
                System.out.println("Key: " + key);
                System.out.printf("Aerospike key: %s\n", translator.getKey());
            }
            WritePolicy wp = null;
            
            Long expireTime = kvp.getExpireTime();
            if (expireTime != null) {
                // Convert expireTime into seconds from now
                long now = new Date().getTime();
                if (now < expireTime) {
                    wp = client.copyWritePolicyDefault();
                    wp.expiration = (int)((expireTime - now) / 1000);
                    if (options.isVerbose()) {
                        System.out.printf("Expire time (ms): %d mapped to expiry time of %s seconds\n", expireTime, wp.expiration);
                    }
                }
                else {
                    // This record has expired, ignore it
                    if (options.isVerbose()) {
                        System.out.println("   Expired!");
                    }
                    return;
                }
            }
            if (options.isVerbose()) {
                System.out.println("Value type: " + kvp.getValueType());
            }
            switch (kvp.getValueType()) {
            case HASH:
            case HASHMAP_AS_LISTPACK:
            case HASHMAP_AS_LISTPACK_EX:
            case HASHMAP_AS_LISTPACK_EX_PRE_GA:
            case HASHMAP_AS_ZIPLIST:
                List<byte[]> values = kvp.getValues();
                
                int length = values.size();
                Bin[] bins = new Bin[length/2];
                for (int i = 0; i < length; i+= 2) {
                    String binName = new String(values.get(i), "ASCII");
                    String binValue = new String(values.get(i+1), "ASCII");
                    bins[i/2] = translator.getBin(binName, binValue);
                }
                if (options.isVerbose()) {
                    System.out.print("Values: ");
                    for (byte[] val : values) {
                        System.out.print("'" + new String(val, "ASCII") + "' ");
                    }
                    System.out.println();
                    System.out.println("------------");
                }
                client.put(wp, translator.getKey(), bins);
                break;
                
            case VALUE:
                String value = new String(kvp.getValues().get(0), "ASCII");
                Bin bin = translator.getBin(null, value);
                client.put(wp, translator.getKey(), bin);
                break;
                
            default:
                if (options.isVerbose()) {
                    System.out.println("----- Unsupported type -----");
                    System.out.print("Values: ");
                    for (byte[] val : kvp.getValues()) {
                        System.out.print("'" + new String(val, "ASCII") + "' ");
                    }
                    System.out.println();
                    System.out.println("------------");
                }
                throw new UnsupportedEncodingException(String.format("Ignoring unsupported type: %s. Key %s\n", key, kvp.getValueType()));
            }
            break;
        }
    }

    private void parseRdbFile(File file) throws Exception {
        try (RdbParser parser = new RdbParser(file)) {
            Entry e;
            while ((e = parser.readNext()) != null) {
                queue.put(e);
            }
        }
    }
    
    private void monitorProgress() throws InterruptedException {
        if (!options.isSilent()) {
            System.out.println("Import started from file: " + options.getInputFileName());
        }
        long startTime = System.currentTimeMillis();
        long lastTotalCount = 0;
        long totalCurrentRecords = 0;
        while (activeThreads.get() > 0) {
            Thread.sleep(1000);
            long success = this.success.get();
            long failure = this.failed.get();
            long recordsThisSecond = success + failure - lastTotalCount;
            totalCurrentRecords = success + failure;
            long now = System.currentTimeMillis();
            long elapsedMilliseconds = now - startTime;
            if (!options.isSilent()) {
                System.out.printf("%,dms: active threads: %d, queue %,d, records processed: %,d (%,d/%,d), throughput: {last second: %,d rps, overall: %,d rps}\n", 
                        elapsedMilliseconds, this.activeThreads.get(), this.queue.size(), totalCurrentRecords, success, failure,
                        recordsThisSecond, (totalCurrentRecords)*1000/elapsedMilliseconds);
            }
            lastTotalCount = totalCurrentRecords;
        }
        this.executor.awaitTermination(7, TimeUnit.DAYS);
        
        System.out.printf("\nExecution completed in %,dms. %,d records imported successfully, %,d records failed.",
                (System.currentTimeMillis()-startTime), success.get(), failed.get());
        if (this.errorFile != null && failed.get() > 0) {
            System.out.printf("Errors appear in %s\n", errorFile.getAbsolutePath());
        }
    }
    
    private void shutdown() {
        if (this.errorWriter != null) {
            this.errorWriter.flush();
            this.errorWriter.close();
        }
    }
    public void run() {
        this.start();
        try {
            this.monitorProgress();
        } catch (InterruptedException ignored) {
        }
        this.shutdown();
    }
    
    public static void main(String[] args) throws Exception {
        AerospikeImporterOptions options = new AerospikeImporterOptions(args);
//        File mapping = new File("/Users/tfaulkes/Programming/eclipseWorkspace/RedisMigrationGuide/src/main/resources/mapping.yaml");
        AerospikeImporter importer = new AerospikeImporter(options);
        importer.run();
        
//        File f = new File("/Users/tfaulkes/dump.rdb");
//        importer.printRdbFile(f);
    }
}

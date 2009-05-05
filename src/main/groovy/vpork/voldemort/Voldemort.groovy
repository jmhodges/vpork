/**
 * 
 */
package vpork.voldemort


import vpork.StatsLogger

import voldemort.client.StoreClient
import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 *
 */
public class Voldemort{
   
    private def cfg
    private List<String> nodes
    private StoreClientFactory storeFact
    private StatsLogger logger
    private Closure    readFactor
    private byte[] bytes
    
    Voldemort(cfg, List<String> nodes, StatsLogger logger) {
        this.cfg           = cfg
        this.nodes         = nodes
        this.logger = logger;
        
        this.readFactor  = cfg.readFactor
        this.bytes       = new byte[cfg.dataSize]
    }
    
    private long now(){
        System.currentTimeMillis()
    }
    
    private String[] generateBootstrapUrls(List<String> hosts, int serverPort) {
        hosts.collect { "tcp://${it}:${serverPort}"} as String[]
    }
    
    void setup() {
        String[] bootstrap = generateBootstrapUrls(nodes, cfg.storePort ?: 6666)
        storeFact = new SocketStoreClientFactory(cfg.storeFactory.coreThreads,
                                               cfg.storeFactory.maxThreads,
                                               cfg.storeFactory.maxQueuedRequests,
                                               cfg.storeFactory.maxConnsPerNode,
                                               cfg.storeFactory.maxTotalConns,
                                               bootstrap)

        /*
        File cfgSnapDir = new File(logDir, voldConfigDir.name)
        cfgSnapDir.mkdirs()
        logAndPrint "Snapshotting voldemort config from ${voldConfigDir.absolutePath} to ${cfgSnapDir}"

        new AntBuilder().copy(toDir: cfgSnapDir) {
            fileset(dir: new File(voldConfigDir, "config"))
        }
        */
    }
    
    void executeIter(StoreClient c, Random r) {
        if (r.nextDouble() < cfg.writeOdds) {
            logger.numWrites.addAndGet(1)
            try {
                storeWrite(c, r)
            } catch(Exception e) {
                // e.printStackTrace()
                logger.writeFails.incrementAndGet()
            }
        }
    
        if (r.nextDouble() < cfg.readOdds) {
            logger.numReads.addAndGet(1)
            try {
                storeRead(c, r)
            } catch(Exception e) {
                // e.printStackTrace()
                logger.readFails.incrementAndGet()
            }
        }
    }

    /**
     * Read from the data store.  We attempt to read values from some
     * time in the past (numRecords is our 'clock').
     *
     * No attempt will be made to read the most recent 'numThreads * 3'
     * records, as it is possible that recent records have not been written to
     * storage, even though numRecords has been incremented.
     */
    void storeRead(StoreClient c, Random r) {
        long timeOffset = 3 * cfg.numThreads
        long curTime = logger.numRecords.get()
        long maxTime = curTime - timeOffset
        if (maxTime < 0) {
            // Haven't collected the minimum records yet.
            return
        }
    
        long recordsAgo = readFactor(r.nextDouble()) * (double)maxTime
        logger.readDistLog.log(recordsAgo)
        // We invert here, because we are more likely to read the most recent
        // record (not the furthest ago)
        long readRec = maxTime - recordsAgo
        String key = "r_${readRec}"
        long start = now()
        def val = c.get(key)
        long time = now() - start
        if (val == null) {
            logger.readsNotFound.addAndGet(1)
        } else {
            logger.bytesRead.addAndGet(val.value.size())
            logger.readTimes << time
            logger.timeReading.addAndGet(time)
            logger.readLog.log(maxTime, time)
        }
    }
  
    void storeWrite(StoreClient c, Random r) {
        long numRecs = logger.numRecords.addAndGet(1)
        String newId = "r_${numRecs}"
        long start = now()
        c.put(newId, bytes)
        logger.bytesWritten.addAndGet(bytes.size())
        long time = now() - start
        logger.writeTimes << time
        logger.timeWriting.addAndGet(time)
        logger.writeLog.log(numRecs, time)         
    }
    
    void execute() {
        // Fire a test shot to see if we can even operate
        logger.logAndPrint "Testing if our store even works ..."
        StoreClient c = storeFact.getStoreClient("bytez")
        c.put("test_${System.currentTimeMillis()}" as String, new byte[1])
        logger.logAndPrint "Giddyup boy!  "
    
        AtomicBoolean shuttingDown = new AtomicBoolean(false)
        Thread.startDaemon {
            double expectedWrites = cfg.numThreads * cfg.threadIters * cfg.writeOdds * cfg.dataSize
            while(!shuttingDown.get()) {
                def percDone = (double) logger.bytesWritten.get() * 100.0 / expectedWrites
                double readGB = (double) logger.bytesRead / (1024 * 1024 * 1024)
                double writeGB = (double) logger.bytesWritten / (1024 * 1024 * 1024)
                logger.logAndPrint sprintf("%%%.2f   num=${logger.numRecords} rGB=%.2f wGB=%.2f rFail=%s wFail=%s notFound=%s",
                        percDone, readGB, writeGB, logger.readFails, logger.writeFails, logger.readsNotFound)
                Thread.sleep(5 * 1000)
            }
        }
    
        logger.start()
        def threads = (0..<cfg.numThreads).collect { threadNo ->
            Thread.start {
                StoreClient client = storeFact.getStoreClient("bytez")
                Random rand = new Random()
    
                cfg.threadIters.times {
                    executeIter(client, rand)
                }
            }
        }
        threads*.join()
        shuttingDown.set(true)
        logger.end()
        logger.printStats()
    }
  
}

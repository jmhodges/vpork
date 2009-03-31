package vpork

import voldemort.client.StoreClient
import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

class VPork {

    private Log log = LogFactory.getLog(VPork)

    private StoreClientFactory storeFact
    private AtomicLong numRecords    = new AtomicLong(0)
    private AtomicLong numWrites     = new AtomicLong(0)
    private AtomicLong numReads      = new AtomicLong(0)
    private AtomicLong bytesWritten  = new AtomicLong(0)
    private AtomicLong bytesRead     = new AtomicLong(0)
    private AtomicLong readsNotFound = new AtomicLong(0)
    private AtomicLong writeFails    = new AtomicLong(0)
    private AtomicLong readFails     = new AtomicLong(0)
    private long       porkStart
    private long       porkEnd
    private StatList   writeTimes   = new StatList()
    private StatList   readTimes    = new StatList()
    private StatFile   writeLog
    private StatFile   readLog
    private StatFile   readDistLog

    private Closure    readFactor
    private def cfg
    private byte[] bytes

    VPork(cfg) {
        this.cfg = cfg
    }

    long now(){
        System.currentTimeMillis()
    }
    
    /**
     * Setup our thread pools, get a store client, prepare everything
     * to run.
     */
    void setup() {
        storeFact = new SocketStoreClientFactory(cfg.storeFactory.coreThreads,
                                                 cfg.storeFactory.maxThreads,
                                                 cfg.storeFactory.maxQueuedRequests,
                                                 cfg.storeFactory.maxConnsPerNode,
                                                 cfg.storeFactory.maxTotalConns,
                                                 cfg.bootstrapUrl as String[])

        readFactor  = cfg.readFactor
        bytes       = new byte[cfg.dataSize]
        readLog     = new StatFile(cfg.readLog as File)
        writeLog    = new StatFile(cfg.writeLog as File)
        readDistLog = new StatFile(cfg.readDistLog as File)

        println "NumThreads      : ${cfg.numThreads}"
        println "Iters / Thread  : ${cfg.threadIters}"
        println "ReadOdds        : ${cfg.readOdds}"
        println "WriteOdds       : ${cfg.writeOdds}"
        println "RewriteOdds     : ${cfg.rewriteOdds}"
        println "Data Size       : ${cfg.dataSize} B"
        println "Write Times     : ${cfg.writeLog}"
        println "Read Times      : ${cfg.readLog}"                
    }

    void executeIter(StoreClient c, Random r) {
        if (r.nextDouble() < cfg.writeOdds) {
            numWrites.addAndGet(1)
            try {
                storeWrite(c, r)
            } catch(Exception e) {
                // e.printStackTrace()
                readFails.incrementAndGet()
            }
        }

        if (r.nextDouble() < cfg.readOdds) {
            numReads.addAndGet(1)
            try {
                storeRead(c, r)
            } catch(Exception e) {
                // e.printStackTrace()
                writeFails.incrementAndGet()
            }
        }
    }

    /**
     * Read from the data store.  We attempt to read values from some
     * time in the past (numRecords is our 'clock').
     *
     * No attempt will be made to read the most recent 'numThreads * 2'
     * records, as it is possible that recent records have not fully been
     * written.
     */
    void storeRead(StoreClient c, Random r) {
        long timeOffset = 2 * cfg.numThreads
        long curTime = numRecords.get()
        long maxTime = curTime - timeOffset
        if (maxTime < 0) {
            // Haven't collected the minimum records yet.
            return
        }

        long recordsAgo = readFactor(r.nextDouble()) * (double)maxTime
        readDistLog.log(recordsAgo)
        // We invert here, because we are more likely to read the most recent
        // record (not the furthest ago)
        long readRec = maxTime - recordsAgo
        //println readRec
        String key = "r_${readRec}"
        //println "$recordsAgo $readRec"
        long start = now()
        def val = c.get(key)
        long time = now() - start
        if (val == null) {
            readsNotFound.addAndGet(1)
        } else {
            bytesRead.addAndGet(val.value.size())
            readTimes << time
            readLog.log(maxTime, time)
        }
    }

    void storeWrite(StoreClient c, Random r) {
        long numRecs = numRecords.addAndGet(1)
        String newId = "r_${numRecs}"
        long start = now()
        c.put(newId, bytes)
        bytesWritten.addAndGet(bytes.size())
        long time = now() - start
        writeTimes << time
        writeLog.log(numRecs, time)         
    }

    void execute() {
        Thread.startDaemon {
            double expectedWrites = cfg.numThreads * cfg.threadIters * cfg.writeOdds * cfg.dataSize
            while(true) {
                Thread.sleep(5 * 1000)
                def percDone = (double)bytesWritten.get() * 100.0 / expectedWrites
                double readGB = (double)bytesRead / (1024 * 1024 * 1024)
                double writeGB = (double)bytesWritten / (1024 * 1024 * 1024)
                log.info sprintf("%%%.2f   num=${numRecords} rGB=%.2f wGB=%.2f rFail=%s wFail=%s notFound=%s rThrough, wThrough",
                                 percDone, readGB, writeGB, readFails, writeFails, readsNotFound)
            }                                                                                   
        }

        porkStart = now()
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
        porkEnd = now()
        readLog.close()
        writeLog.close()
        readDistLog.close()
        printStats()
    }

    void printStats() {
        long elapsed = porkEnd - porkStart
        printf "-------------\n"
        printf "Elapsed time:  %s sec\n", elapsed / 1000
        printf "\n"
        printf "Writes:\n"
        printf "  Num Writes:           ${numWrites}\n"
        printf "  Write Failures:       %s\n", writeFails
        printf "  Write Latency:        %.2f ms\n", writeTimes.average
        printf "  Write Latency (99):   %.2f ms\n", writeTimes.getPercentile(0.99)
        printf "  Bytes Written:        %.2f MB\n", bytesWritten / (1024 * 1024)
        printf "  Write Throughput:     %.2f KB / ms\n", (double)(bytesWritten / 1024.0) / (double)elapsed        
        printf "\n"
        printf "Reads:\n"
        printf "  Num Read:             ${numReads}\n"
        printf "  Read Failures:        %s\n", readFails
        printf "  Read Latency:         %.2f ms\n", readTimes.average
        printf "  Read Latency (99):    %.2f ms\n", readTimes.getPercentile(0.99)
        printf "  Read Not Found:       %s (%%%.2f)\n", readsNotFound, (double)readsNotFound * 100.0 / (double)numReads
        printf "  Bytes Read:           %.2f MB\n", bytesRead / (1024 * 1024)
        printf "  Read Throughput:      %.2f KB / ms\n", (double)(bytesRead / 1024.0) / (double)elapsed
    }

    static void main(String[] args) {
        def cfg = new ConfigSlurper().parse(new File(args[0]).toURL())

        VPork vp = new VPork(cfg)
        vp.setup()
        vp.execute()
    }
}

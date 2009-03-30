package vpork

import voldemort.client.StoreClient
import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import java.util.concurrent.atomic.AtomicLong

class VPork {
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

        readFactor = cfg.readFactor
        bytes      = new byte[cfg.dataSize]
        readLog    = new StatFile(cfg.readLog as File)
        writeLog   = new StatFile(cfg.writeLog as File)        

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

    void storeRead(StoreClient c, Random r) {
        long curTime = numRecords.get() - 1
        long recordsAgo = readFactor(r.nextDouble()) * (double)curTime

        // We invert here, because we are more likely to read the most recent
        // record (not the furthest ago)
        long readRec = curTime - recordsAgo
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
            readLog.log(curTime, time)
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
        printf "  Write Latency (99.9): %.2f ms\n", writeTimes.getPercentile(0.999)
        printf "  Bytes Written:        %.2f MB\n", bytesWritten / (1024 * 1024)
        printf "  Write Throughput:     %.2f KB / ms\n", (double)(bytesWritten / 1024.0) / (double)elapsed        
        printf "\n"
        printf "Reads:\n"
        printf "  Num Read:             ${numReads}\n"
        printf "  Read Failures:        %s\n", readFails
        printf "  Read Latency:         %.2f ms\n", readTimes.average
        printf "  Read Latency (99.9):  %.2f ms\n", readTimes.getPercentile(0.999)
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

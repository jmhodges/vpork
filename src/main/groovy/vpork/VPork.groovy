/*
 * Copyright 2009 Hyperic, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Jon Travis (jon.travis@hyperic.com)
 */
package vpork

import voldemort.client.StoreClient
import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import java.util.concurrent.atomic.AtomicLong
import java.text.DateFormat

class VPork {
    private StoreClientFactory storeFact
    private List<String> nodes
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
    private File       logDir
    private Writer     progressLog

    private Closure    readFactor
    private def cfg
    private byte[] bytes

    VPork(cfg, List<String> nodes) {
        this.cfg   = cfg
        this.nodes = nodes
    }

    long now(){
        System.currentTimeMillis()
    }

    private String[] generateBootstrapUrls(List<String> hosts, int serverPort) {
        hosts.collect { "tcp://${it}:${serverPort}"} as String[]
    }

    private File makeLogDir() {
        File res = cfg.logDir ?: 'results' as File

        int i=0
        while(true) {
            File subDir = new File(res, "${cfg.testName}-${i++}")
            if (!subDir.exists()) {
                if (subDir.mkdirs() == false) {
                    println "Error creating ${subDir.absolutePath}"
                    return null
                }
                return subDir
            }
        }
    }

    /**
     * Setup our thread pools, get a store client, prepare everything
     * to run.
     */
    void setup() {
        String[] bootstrap = generateBootstrapUrls(nodes, cfg.storePort ?: 6666)
        storeFact = new SocketStoreClientFactory(cfg.storeFactory.coreThreads,
                                                 cfg.storeFactory.maxThreads,
                                                 cfg.storeFactory.maxQueuedRequests,
                                                 cfg.storeFactory.maxConnsPerNode,
                                                 cfg.storeFactory.maxTotalConns,
                                                 bootstrap)


        logDir  = makeLogDir()
        println "Writing results to: ${logDir.absolutePath}"

        readFactor  = cfg.readFactor
        bytes       = new byte[cfg.dataSize]
        readLog     = new StatFile(new File(logDir, "read.log"))
        writeLog    = new StatFile(new File(logDir, "write.log"))
        readDistLog = new StatFile(new File(logDir, "read_dist.log"))
        progressLog = new FileWriter(new File(logDir, "progress.log"))

        logAndPrint "NumThreads      : ${cfg.numThreads}"
        logAndPrint "Iters / Thread  : ${cfg.threadIters}"
        logAndPrint "ReadOdds        : ${cfg.readOdds}"
        logAndPrint "WriteOdds       : ${cfg.writeOdds}"
        logAndPrint "RewriteOdds     : ${cfg.rewriteOdds}"
        logAndPrint "Data Size       : ${cfg.dataSize} B"
        logAndPrint "BoostrapURLs    : ${bootstrap.join(',')}"
    }

    private void logAndPrint(String s) {
        DateFormat fmt = DateFormat.getDateTimeInstance()
        s = "${fmt.format(new Date())} - ${s}\n"
        synchronized (progressLog) {
            progressLog.write(s)
            progressLog.flush()
        }
        print s
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
     * No attempt will be made to read the most recent 'numThreads * 3'
     * records, as it is possible that recent records have not been written to
     * storage, even though numRecords has been incremented.
     */
    void storeRead(StoreClient c, Random r) {
        long timeOffset = 3 * cfg.numThreads
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
        String key = "r_${readRec}"
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
        def statThread = Thread.startDaemon {
            double expectedWrites = cfg.numThreads * cfg.threadIters * cfg.writeOdds * cfg.dataSize
            while(true) {
                Thread.sleep(5 * 1000)
                def percDone = (double)bytesWritten.get() * 100.0 / expectedWrites
                double readGB = (double)bytesRead / (1024 * 1024 * 1024)
                double writeGB = (double)bytesWritten / (1024 * 1024 * 1024)
                logAndPrint sprintf("%%%.2f   num=${numRecords} rGB=%.2f wGB=%.2f rFail=%s wFail=%s notFound=%s",
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
        ([statThread] + threads)*.join()
        porkEnd = now()
        readLog.close()
        writeLog.close()
        readDistLog.close()
        progressLog.close()
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
        if (args.length != 2) {
            println "Syntax:  vpork <vporkConfig.groovy> <voldemortConfigDir>"
            return
        }

        def cfg = new ConfigSlurper().parse(new File(args[0]).toURL())
        File voldCfgDir = args[1] as File
        println "Looking in ${args[1]} for config/nodes.xml"
        File nodesFile = new File(new File(voldCfgDir, "config"), "nodes")

        if (!nodesFile.isFile()) {
            println "Unable to read nodes file: ${nodesFile.absolutePath}"
            return
        }

        List<String> nodes = nodesFile.readLines()
        VPork vp = new VPork(cfg, nodes)
        vp.setup()
        vp.execute()
    }
}

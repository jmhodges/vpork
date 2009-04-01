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
import java.util.concurrent.atomic.AtomicBoolean

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
    private AtomicLong timeWriting   = new AtomicLong(0)
    private AtomicLong timeReading   = new AtomicLong(0)
    private long       porkStart
    private long       porkEnd
    private StatList   writeTimes   = new StatList()
    private StatList   readTimes    = new StatList()
    private StatFile   writeLog
    private StatFile   readLog
    private StatFile   readDistLog
    private File       logDir
    private File       voldConfigDir
    private Writer     progressLog

    private Closure    readFactor
    private def cfg
    private byte[] bytes

    VPork(cfg, List<String> nodes, File voldConfigDir) {
        this.cfg           = cfg
        this.nodes         = nodes
        this.voldConfigDir = voldConfigDir
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

        File cfgSnapDir = new File(logDir, voldConfigDir.name)
        cfgSnapDir.mkdirs()
        logAndPrint "Snapshotting voldemort config from ${voldConfigDir.absolutePath} to ${cfgSnapDir}"

        new AntBuilder().copy(toDir: cfgSnapDir) {
            fileset(dir: new File(voldConfigDir, "config"))
        }
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
                writeFails.incrementAndGet()
            }
        }

        if (r.nextDouble() < cfg.readOdds) {
            numReads.addAndGet(1)
            try {
                storeRead(c, r)
            } catch(Exception e) {
                // e.printStackTrace()
                readFails.incrementAndGet()
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
            timeReading.addAndGet(time)
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
        timeWriting.addAndGet(time)
        writeLog.log(numRecs, time)         
    }

    void execute() {
        // Fire a test shot to see if we can even operate
        logAndPrint "Testing if our store even works ..."
        StoreClient c = storeFact.getStoreClient("bytez")
        c.put("test_${System.currentTimeMillis()}" as String, new byte[1])
        logAndPrint "Giddyup boy!  "

        AtomicBoolean shuttingDown = new AtomicBoolean(false)
        Thread.startDaemon {
            double expectedWrites = cfg.numThreads * cfg.threadIters * cfg.writeOdds * cfg.dataSize
            while(true) {
                Thread.sleep(5 * 1000)
                if (shuttingDown.get()) {
                    return
                }
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
        threads*.join()
        shuttingDown.set(true)
        porkEnd = now()
        printStats()        
        readLog.close()
        writeLog.close()
        readDistLog.close()
        progressLog.close()

    }

    void printStats() {
        long elapsed = porkEnd - porkStart
        logAndPrint sprintf("-------------")
        logAndPrint sprintf("Elapsed time:  %s sec", elapsed / 1000)
        logAndPrint ""
        logAndPrint sprintf("Writes:")
        logAndPrint sprintf("  Num Writes:           ${numWrites}")
        logAndPrint sprintf("  Write Throughput:     %.2f writes / sec", (double)numWrites * 1000.0 / (double)elapsed)
        logAndPrint sprintf("  Write Failures:       %s", writeFails)
        logAndPrint sprintf("  Write Latency:        %.2f ms", writeTimes.average)
        logAndPrint sprintf("  Write Latency (%%99):  %.2f ms", writeTimes.getPercentile(0.99))
        logAndPrint sprintf("  Bytes Written:        %.2f MB", bytesWritten / (1024 * 1024))
        logAndPrint sprintf("  Thread w/Throughput:  %.2f KB / sec", (double)(bytesWritten / 1024.0) * 100.0 / (double)timeWriting.get())
        logAndPrint sprintf("  Total w/Throughput:   %.2f KB / sec", (double)(bytesWritten / 1024.0) * 100.0 / (double)elapsed)
        logAndPrint ""
        logAndPrint sprintf("Reads:")
        logAndPrint sprintf("  Num Read:             ${numReads}")
        logAndPrint sprintf("  Read Throughput:      %.2f reads / sec", (double)numReads * 1000.0 / (double)elapsed)        
        logAndPrint sprintf("  Read Failures:        %s", readFails)
        logAndPrint sprintf("  Read Latency:         %.2f ms", readTimes.average)
        logAndPrint sprintf("  Read Latency (%%99):   %.2f ms", readTimes.getPercentile(0.99))
        logAndPrint sprintf("  Read Not Found:       %s (%%%.2f)", readsNotFound, (double)readsNotFound * 100.0 / (double)numReads)
        logAndPrint sprintf("  Bytes Read:           %.2f MB", bytesRead / (1024 * 1024))
        logAndPrint sprintf("  Thread r/Throughput:  %.2f KB / sec", (double)(bytesRead / 1024.0) * 100.0 / (double)timeReading.get())
        logAndPrint sprintf("  Total r/Throughput:   %.2f KB / sec", (double)(bytesRead / 1024.0) * 100.0 / (double)elapsed)
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
        VPork vp = new VPork(cfg, nodes, voldCfgDir)
        vp.setup()
        vp.execute()
    }
}

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

import vpork.voldemort.VoldemortClientFactory
import vpork.cassandra.CassandraClientFactory
import vpork.memory.MemoryClientFactory
import vpork.hbase.HbaseThriftClientFactory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.ThreadPoolExecutor
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.Logger


class VPork {

    private ConfigObject cfg
    private HashClientFactory clientFactory 
    private StatsLogger logger
    private List<String>factoryArgs

    VPork(ConfigObject cfg, HashClientFactory storage,
          StatsLogger logger, List<String>factoryArgs)
    {
        this.cfg = cfg
        this.clientFactory = storage
        this.logger = logger
        this.factoryArgs = factoryArgs
    }

     /**
     * Setup our thread pools, get a store client, prepare everything
     * to run.
     */
    void setup() {       
        logger.setup()
        clientFactory.setup(cfg, logger, factoryArgs)
    }

    void execute() {
        AtomicBoolean shuttingDown = new AtomicBoolean(false)
        testPorkerConnection()
        startLoggerThread(shuttingDown)

        logger.start()

        ExecutorService executor = startPorkerThreads()
        
        executor.shutdown()
        executor.awaitTermination(60 * 60 * 2, TimeUnit.SECONDS) // 2 hours
        shuttingDown.set(true)
        logger.end()
        logger.printStats()
    }

    private void startLoggerThread(AtomicBoolean shuttingDown) {
        Thread.startDaemon {
            double expectedWrites = cfg.threadIters * cfg.writeOdds * cfg.dataSize
            while(!shuttingDown.get()) {
                def percDone = (double) logger.bytesWritten.get() * 100.0 / expectedWrites
                double readGB = (double) logger.bytesRead / (1024 * 1024 * 1024)
                double writeGB = (double) logger.bytesWritten / (1024 * 1024 * 1024)
                logger.logAndPrint sprintf("%%%.2f   num=${logger.numRecords} rGB=%.2f wGB=%.2f rFail=%s wFail=%s notFound=%s",
                        percDone, readGB, writeGB, logger.readFails, logger.writeFails, logger.readsNotFound)
                Thread.sleep(5 * 1000)
            }
        }
    }

    private void testPorkerConnection() {
        new Porker(clientFactory.createClient(), cfg, logger).testSetup()
    }

    private ExecutorService startPorkerThreads() {
        BlockingQueue workQueue = new ArrayBlockingQueue(20000)
        Executor executor = new ThreadPoolExecutor(cfg.numThreads,
                                                   cfg.numThreads,
                                                   Long.MAX_VALUE, TimeUnit.MILLISECONDS,
                                                   workQueue)

        // Fair queue of porkers
        BlockingQueue<Porker> porkers = new ArrayBlockingQueue(cfg.numThreads, true)
        (0..<cfg.numThreads).each { threadNo ->
            HashClient client = clientFactory.createClient()
            Porker porker = new Porker(client, cfg, logger)
            porkers.put(porker)
        }

        cfg.threadIters.times {
            while (workQueue.size() > 10000) {
                sleep(1000)
                continue
            }
            
            executor.execute() {
                Porker porker = porkers.take()
                try {
                    porker.executeIter()
                } finally {
                    porkers.put(porker)
                }
                
            }
        }
        executor
    }

    void close() {
        logger.close()
    }

    private static HashClientFactory loadFactory(String storageType) {
        if(storageType == "cassandra") {
            return new CassandraClientFactory()
        } else if(storageType == "voldemort") {
            return new VoldemortClientFactory()
        } else if(storageType == "memory") {
            return new MemoryClientFactory()
        } else if(storageType == "hbase-thrift") {
            return new HbaseThriftClientFactory()
        } else {
            return null
        }
    }

    static void main(String[] args) {
        if (args.length < 1) {
            println "Syntax:  vpork <configFile> [nodesFile]"
            println ""
            println "Example: vpork configs/memory/30-thread-pork.groovy"
            println ""
            println "Where config/memory/nodes is a flat file, each line containing a remote"
            println "node to test against"
            return
        }

        BasicConfigurator.configure()
        LogManager.rootLogger.level = Level.INFO
        LogManager.getLogger("voldemort").level = Level.INFO

        ConfigObject cfg = new ConfigSlurper().parse(new File(args[0]).toURL())

        StatsLogger logger = new StatsLogger(cfg)
        HashClientFactory storage = loadFactory(cfg.storageType)

        List factoryArgs = args[1..<args.length]
        VPork vp = new VPork(cfg, storage, logger, factoryArgs)

        try {
            vp.setup()
        } catch(SetupException exc) {
            println "Error running VPork.  Setup exception"
            println "**: ${exc.message}"
            return
        }
        vp.execute()
        vp.close()
    }
}

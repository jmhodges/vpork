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

import vpork.voldemort.Voldemort
import vpork.cassandra.Cassandra

import java.util.concurrent.atomic.AtomicBoolean

class VPork {
    
    private def cfg
    private def storage //the system we're testing
    private StatsLogger logger

    VPork(cfg, storage, StatsLogger logger) {
        this.cfg = cfg
        this.storage = storage
        this.logger = logger
    }

     /**
     * Setup our thread pools, get a store client, prepare everything
     * to run.
     */
    void setup() {       
        logger.setup()
        storage.setup()
    }

    private void startLoggerThread(AtomicBoolean shuttingDown) {
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
    }

    void execute() {
        AtomicBoolean shuttingDown = new AtomicBoolean(false)
        testPorkerConnection()
        startLoggerThread(shuttingDown)

        logger.start()

        List<Thread> threads = startPorkerThreads()
        threads*.join()

        shuttingDown.set(true)
        logger.end()
        logger.printStats()
    }

    private void testPorkerConnection() {
        new Porker(storage.createClient(), cfg, logger).testSetup()
    }

    private List<Thread> startPorkerThreads() {
        (0..<cfg.numThreads).collect { threadNo ->
            Thread.start {
                def client = storage.createClient()
                Porker porker = new Porker(client, cfg, logger)

                cfg.threadIters.times {
                    porker.executeIter()
                }
            }
        }
    }

    void close() {
        logger.close()
    }
    
    static void main(String[] args) {
        if (args.length < 2) {
            println "Syntax:  vpork <vporkConfig.groovy> <nodesFile>"
            return
        }

        def cfg = new ConfigSlurper().parse(new File(args[0]).toURL())
        File nodesFile = args[1] as File

        if (!nodesFile.isFile()) {
            println "Unable to read nodes file: ${nodesFile.absolutePath}"
            return
        }

        List<String> nodes = nodesFile.readLines()
        
        StatsLogger logger = new StatsLogger(cfg)
        def storage = null;
        
        if("cassandra" == cfg.storageType) {
            storage = new Cassandra(cfg, nodes)
        } else if("voldemort" == cfg.storageType) {
            storage = new Voldemort(cfg, nodes, logger)
        } else {
            println "Storage type not supported: ${cfg.storageType}"
            return
        }
        
        VPork vp = new VPork(cfg, storage, logger)
        vp.setup()
        vp.execute()
        vp.close()
    }
}

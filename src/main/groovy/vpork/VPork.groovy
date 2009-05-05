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
//import vpork.cassandra.Cassandra

class VPork {
    
    private def cfg
    
    private Voldemort voldemort
    private StatsLogger logger

    VPork(cfg, Voldemort voldemort, StatsLogger logger) {
        this.cfg = cfg
        this.voldemort = voldemort
        this.logger = logger
    }

     /**
     * Setup our thread pools, get a store client, prepare everything
     * to run.
     */
    void setup() {       
        logger.setup()
        voldemort.setup()
    }
    
    void execute() {
        voldemort.execute()
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
        VPork vp = null;
        if("cassandra" == cfg.storageType) {
            
        } else if("voldemort" == cfg.storageType) {
            Voldemort voldemort = new Voldemort(cfg, nodes, logger)
            vp = new VPork(cfg, voldemort, logger)
        } else {
            println "Storage type not supported: ${cfg.storageType}"
            return
        }
        
        vp.setup()
        vp.execute()
        vp.close()
    }
}

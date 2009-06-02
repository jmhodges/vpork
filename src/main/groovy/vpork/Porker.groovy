package vpork

public class Porker {
    private HashClient hash
    private def cfg
    private StatsLogger logger
    private Closure readFactor
    private byte[] bytes


    public Porker(HashClient hash, cfg, StatsLogger logger) {
        this.hash = hash;
        this.cfg    = cfg;
        this.logger = logger;
        
        this.readFactor  = cfg.readFactor
        this.bytes       = new byte[cfg.dataSize]
    }
  
    private long now(){
        System.currentTimeMillis()
    }
    
    /**
     * Check that the voldemort server is running
     */
    void testSetup() {
        // Fire a test shot to see if we can even operate
        logger.logAndPrint "Testing if our store even works ..."
        hash.put("test_${System.currentTimeMillis()}" as String, new byte[1])
        logger.logAndPrint "Giddyup boy!  "
    }
    
    void executeIter(Random r) {
        if (r.nextDouble() < cfg.writeOdds) {
            logger.numWrites.addAndGet(1)
            try {
                storeWrite(r)
            } catch(Exception e) {
                //e.printStackTrace()
                logger.writeFails.incrementAndGet()
            }
        }
    
        if (r.nextDouble() < cfg.readOdds) {
            logger.numReads.addAndGet(1)
            try {
                storeRead(r)
            } catch(Exception e) {
                //e.printStackTrace()
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
    void storeRead(Random r) {
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
        def val = hash.get(key)
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
  
    void storeWrite(Random r) {
        long numRecs = logger.numRecords.addAndGet(1)
        String newId = "r_${numRecs}"
        long start = now()
        hash.put(newId, bytes)
        logger.bytesWritten.addAndGet(bytes.size())
        long time = now() - start
        logger.writeTimes << time
        logger.timeWriting.addAndGet(time)
        logger.writeLog.log(numRecs, time)
    }    
}

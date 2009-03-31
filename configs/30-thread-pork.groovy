/**
 * Configuration of the Voldemort ClientStore.
 */
storeFactory {
    coreThreads       = 30
    maxThreads        = 30
    maxQueuedRequests = 100
    maxConnsPerNode   = 30
    maxTotalConns     = 30
}

/**
 * Where do we bootstrap from?
 */
bootstrapUrl = ["tcp://voldhost:6666"]

// # of client threads to start
numThreads  = 30

// Odds of performing a read op per iteration (1 = 100%)
readOdds    = 0.1
writeOdds   = 0.8
rewriteOdds = 0.1

// How many iterations should each thread execute?
threadIters = 2000

// How much data should be written per write operation?
dataSize    = (8 + 4) * 2000

// What function best describes our data access patterns?  This factor is
// is applied across the range of already-written records.  We choose a function
// with a long tail, since we do most of our reads at current-time, and
// not many in the recent past.
readFactor = {x -> (1.0 - x) ** 10.0}

// Where to write read activity (x=recNo, y=ms to read)
readLog     = "read.log"

// Where to store read distribution  (x=current time, y=distance from current time)
readDistLog = "readDist.log"

// Where to write write
writeLog    = "write.log"

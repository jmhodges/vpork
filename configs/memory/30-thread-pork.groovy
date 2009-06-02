storageType = "memory"

// Client worker threads
numThreads = 30

testName="$storageType-$numThreads-thread-pork"

/**
 * Configuration of the Voldemort ClientStore.
 */
storeFactory {
    maxThreads        = 100
    maxConnsPerNode   = 100
    storePort         = 6666
//    maxTotalConns     = 500
//    maxQueuedRequests = 100
}


// Odds of performing a read op per iteration (1 = 100%)
readOdds    = 0.0
writeOdds   = 1.0

// How many iterations should each thread execute?
threadIters = 10000

// How much data should be written per write operation?
dataSize    = 5000

// What function best describes our data access patterns?  This factor is
// is applied across the range of already-written records.  We choose a function
// with a long tail, since we do most of our reads at current-time, and
// not many in the recent past.
readFactor = {x -> (1.0 - x) ** 10.0}

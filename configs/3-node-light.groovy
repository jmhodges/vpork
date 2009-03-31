storeFactory {
    coreThreads       = 30
    maxThreads        = 30
    maxQueuedRequests = 100
    maxConnsPerNode   = 30
    maxTotalConns     = 30
}

bootstrapUrl = ["tcp://localhost:6666"]

numThreads  = 10
readOdds    = 0.1
writeOdds   = 0.8
rewriteOdds = 0.1
threadIters = 2000
dataSize    = (8 + 4) * 2000

readFactor = {x -> (1.0 - x) ** 20.0}

readLog  = "read.log"
readDistLog = "readDist.log"
writeLog = "write.log"

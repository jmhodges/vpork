workDir = '/tmp/vpork'
voldemortHome = '/Users/jtravis/dev/project-voldemort'

name = "3-node-light"

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
threadIters = 20000
dataSize    = (8 + 4) * 2000

readFactor = {x -> (1.0 - x) ** 20.0}

readLog  = "read.log"
writeLog = "write.log"

partitions        = 100
replicationFactor = 1
requiredReads     = 1
requiredWrites    = 1

serverProps {
    max.threads   = 100
    http.enable   = true
    socket.enable = true

    bdb {
        sync.transactions = false
        cache.size = '100MB'
    }
}

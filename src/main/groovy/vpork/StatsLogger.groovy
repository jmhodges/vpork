package vpork


import java.util.concurrent.atomic.AtomicLong
import java.text.DateFormat

public class StatsLogger {

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
  
    private StatList   writeTimes
    private StatList   readTimes
    private StatFile   writeLog
    private StatFile   readLog
    private StatFile   readDistLog
    
    private Writer     progressLog
    
    private File       logDir

    private long       porkStart
    private long       porkEnd
    
    def cfg
    
    StatsLogger(cfg) {
        this.cfg = cfg
    }
    
    void setup() {
        writeTimes = new StatList(cfg.numThreads * cfg.threadIters)
        readTimes  = new StatList(cfg.numThreads * cfg.threadIters)

        logDir  = makeLogDir()
        println "Writing results to: ${logDir.absolutePath}"
        
        readLog     = new StatFile(new File(logDir, "read.log"))
        writeLog    = new StatFile(new File(logDir, "write.log"))
        readDistLog = new StatFile(new File(logDir, "read_dist.log"))
        progressLog = new FileWriter(new File(logDir, "progress.log"))
        
        logAndPrint "NumThreads      : ${cfg.numThreads}"
        logAndPrint "Iterations      : ${cfg.threadIters}"
        logAndPrint "ReadOdds        : ${cfg.readOdds}"
        logAndPrint "WriteOdds       : ${cfg.writeOdds}"
        logAndPrint "RewriteOdds     : ${cfg.rewriteOdds}"
        logAndPrint "Data Size       : ${cfg.dataSize} B"
        // logAndPrint "BoostrapURLs    : ${bootstrap.join(',')}"      
    }
    
    void start() {
        porkStart = System.currentTimeMillis()
    }
    
    void end() {
        porkEnd = System.currentTimeMillis()
    }
    
    void close() {
        readLog.close()
        writeLog.close()
        readDistLog.close()
        progressLog.close()
    }

    File getLogDir() {
        this.logDir
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
        logAndPrint sprintf("  Write Latency (%%99):  %.2f ms", writeTimes.getPercentile(99))
        logAndPrint sprintf("  Write Latency stdDev: %.2f", writeTimes.getStandardDeviation())
        logAndPrint sprintf("  Bytes Written:        %.2f MB", bytesWritten / (1024 * 1024))
        logAndPrint sprintf("  Thread w/Throughput:  %.2f KB / sec", (double)(bytesWritten / 1024.0) / (double)timeWriting.get())
        logAndPrint sprintf("  Total w/Throughput:   %.2f KB / sec", (double)(bytesWritten / 1024.0) / ((double)elapsed / 1000))
        logAndPrint ""
        logAndPrint sprintf("Reads:")
        logAndPrint sprintf("  Num Read:             ${numReads}")
        logAndPrint sprintf("  Read Throughput:      %.2f reads / sec", (double)numReads * 1000.0 / (double)elapsed)        
        logAndPrint sprintf("  Read Failures:        %s", readFails)
        logAndPrint sprintf("  Read Latency:         %.2f ms", readTimes.average)
        logAndPrint sprintf("  Read Latency (%%99):   %.2f ms", readTimes.getPercentile(99))
        logAndPrint sprintf("  Read Latency stdDev:  %.2f", readTimes.getStandardDeviation())
        logAndPrint sprintf("  Read Not Found:       %s (%%%.2f)", readsNotFound, (double)readsNotFound * 100.0 / (double)numReads)
        logAndPrint sprintf("  Bytes Read:           %.2f MB", bytesRead / (1024 * 1024))
        logAndPrint sprintf("  Thread r/Throughput:  %.2f KB / sec", (double)(bytesRead / 1024.0) / (double)timeReading.get())
        logAndPrint sprintf("  Total r/Throughput:   %.2f KB / sec", (double)(bytesRead / 1024.0) / ((double)elapsed / 1000))
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
    
}

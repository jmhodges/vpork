package vpork

class NodesUtil {
    static List<String> loadNodes(StatsLogger log, List<String>nodesArg) {
        log.logAndPrint "Nodes file given: $nodesArg"
        if (nodesArg.isEmpty()) {
            log.logAndPrint "No nodes file specified.  Using localhost"
            return ['localhost']
        }

        File nodesFile = nodesArg[0] as File
        log.logAndPrint "Attempting to read nodes file: ${nodesFile.absolutePath}"
        if (!nodesFile.isFile()) {
            throw new SetupException("Unable to read nodes file: ${nodesFile.absolutePath}")
        }

        List res = nodesFile.readLines()
        res.each {
            log.logAndPrint "Using node: $it"
        }
        res
    }
}
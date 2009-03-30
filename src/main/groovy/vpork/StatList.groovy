package vpork

import java.util.concurrent.ConcurrentLinkedQueue

class StatList {
    ConcurrentLinkedQueue<Double> vals = []

    def leftShift(double b) {
        vals.add(b)
    }

    def getAverage() {
        double total = 0
        vals.each { total += it }
        total / vals.size()
    }

    double getPercentile(double p) {
        List vList = vals.sort()
        int idx = p * (double)vList.size()
        vList[idx]
    }
}

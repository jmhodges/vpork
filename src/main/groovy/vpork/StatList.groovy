package vpork

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation

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
        if (!vals) {
            return Double.NaN
        }
        
        List vList = vals.sort()
        int idx = p * (double)vList.size()
        vList[idx]
    }
    
    double getStandardDeviation() {
        double[] data = vals.toArray()
        StandardDeviation sd = new StandardDeviation();
        return sd.evaluate(data)
    }
}

package vpork

import org.apache.commons.math.stat.descriptive.moment.StandardDeviation
import org.apache.commons.math.stat.descriptive.rank.Percentile
import java.util.concurrent.ArrayBlockingQueue

class StatList {
    private Queue<Double> vals

    StatList(int maxSize) {
        vals = new ArrayBlockingQueue(maxSize)
    }

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
        new Percentile(p).evaluate(vList as double[])
    }
    
    double getStandardDeviation() {
        new StandardDeviation().evaluate(vals as double[])
    }
}

package vpork

class StatFile {
    Writer w

    StatFile(File out) {
        w = new BufferedWriter(new FileWriter(out))
    }

    void log(double x, double y) {
        String s = x + "," + y + "\n"
        synchronized (w) {
            w.write(s)
        }
    }

    void close() {
        w.flush()
        w.close()
    }
}

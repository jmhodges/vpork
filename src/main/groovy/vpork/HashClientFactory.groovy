package vpork

interface HashClientFactory {
    void setup()

    HashClient createClient()
}
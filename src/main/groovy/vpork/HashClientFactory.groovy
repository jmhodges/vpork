package vpork

interface HashClientFactory {
    void setup(ConfigObject cfg, StatsLogger logger, List<String>factoryArgs)

    HashClient createClient()
}

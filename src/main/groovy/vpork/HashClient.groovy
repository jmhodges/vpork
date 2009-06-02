package vpork

interface HashClient {
    /**
     * Get the value of the key from the hash.
     *
     * @return null if no value exists for the key
     */
    byte[] get(String key)

    void put(String key, byte[] value)
}

package vpork.memory;

import vpork.HashClient;

import java.util.Map
import java.util.concurrent.ConcurrentHashMap;

/**
 */
class Memory {
    private Map hash = new ConcurrentHashMap()

    HashClient createClient() {
        [ get : { key -> hash[key] },
          put : { key, value -> hash[key] = value }
        ] as HashClient
    }

    void setup() {
    }
}

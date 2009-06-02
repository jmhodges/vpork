package vpork.voldemort

import vpork.HashClient
import voldemort.client.StoreClient


class VoldemortAdapter implements HashClient {
    private StoreClient client

    VoldemortAdapter(StoreClient client) {
        this.client = client
    }

	byte[] get(String key) {
        client.get(key)?.value
	}

	void put(String key, byte[] value) {
        client.put(key, (Object)value)
	}
}

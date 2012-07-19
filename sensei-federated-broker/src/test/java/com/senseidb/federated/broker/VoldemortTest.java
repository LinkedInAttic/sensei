package com.senseidb.federated.broker;

import voldemort.client.MockStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.serialization.StringSerializer;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.store.versioned.VersionIncrementingStore;
import voldemort.utils.SystemTime;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;

public class VoldemortTest {
  public static void main(String[] args) {
    Store<String, String> store = new VersionIncrementingStore<String, String>(new InMemoryStorageEngine<String,String>("test"),
        0, new SystemTime());
    store = new InconsistencyResolvingStore<String, String>(store, new VectorClockInconsistencyResolver<String>());
    store.put("jaba", new Versioned<String>("baba"));
    System.out.println(store.get("jaba").get(0).getValue());
    StoreClient<Object, Object> storeClient = new MockStoreClientFactory(new StringSerializer(), new StringSerializer()).getStoreClient("test");
    storeClient.put("jaba", "baba");
    System.out.println(storeClient.get("jaba").getValue());
  }
}

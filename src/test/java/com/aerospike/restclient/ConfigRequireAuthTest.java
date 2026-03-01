package com.aerospike.restclient;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest(properties = "aerospike.restclient.requireAuthentication=true")
public class ConfigRequireAuthTest {
    @Test
    public void startUpTest() {
        // Tests that the application will startup even without an default AerospikeClient instantiated.
    }
}

package com.fyntrac.common;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "memcached.host=127.0.0.1",
    "spring.data.mongodb.host=127.0.0.1",
    "spring.pulsar.client.service-url=pulsar://127.0.0.1:6650"
})
class CommonApplicationTests {

	@Test
	void contextLoads() {
	}

}

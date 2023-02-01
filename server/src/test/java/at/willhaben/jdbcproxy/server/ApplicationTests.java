package at.willhaben.jdbcproxy.server;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ApplicationTests {

	/**
	 * Just verify that the spring context starts up.
	 */
	@Test
	void contextLoads() {
		Assert.assertTrue(Boolean.TRUE);
	}

}

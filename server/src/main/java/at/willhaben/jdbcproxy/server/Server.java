package at.willhaben.jdbcproxy.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.ldap.LdapAutoConfiguration;

/**
 * Main class (startup code) for the proxy server.
 */
@SpringBootApplication(exclude = {LdapAutoConfiguration.class, DataSourceAutoConfiguration.class})
public class Server {

	public static void main(String[] args) {
		SpringApplication.run(Server.class, args);
	}

}

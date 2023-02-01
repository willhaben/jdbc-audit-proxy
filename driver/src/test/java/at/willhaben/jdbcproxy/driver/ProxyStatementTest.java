package at.willhaben.jdbcproxy.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class ProxyStatementTest {

    @Mock
    private Method method;

    @Test
    void testCloseConnection() throws Throwable {
        Mockito.when(method.getName()).thenReturn("close");
        ProxyStatement proxyStatement = new ProxyStatement(null, null);
        proxyStatement.invoke(null, method, null);

        assertThrows(SQLException.class,
                () -> proxyStatement.invoke(null, method, null),
                "Statement is closed"
        );
    }

}

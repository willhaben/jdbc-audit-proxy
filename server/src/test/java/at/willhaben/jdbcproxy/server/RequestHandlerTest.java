package at.willhaben.jdbcproxy.server;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class RequestHandlerTest {
    @Test
    public void testDateMapping() {
        var rh = new RequestHandler(null, null, 0, null, null, null);
        var ld = LocalDateTime.parse("2022-06-01T13:14:15");
        java.sql.Date date = new java.sql.Date(ld.toInstant(ZoneOffset.UTC).toEpochMilli());
        var result = rh.mapToType(date, Types.DATE);
        Assert.assertTrue(result instanceof String);
        Assert.assertEquals("2022-06-01T13:14:15", result);
    }

    @Test
    public void testTimestampMapping() {
        var rh = new RequestHandler(null, null, 0, null, null, null);
        var ld = LocalDateTime.parse("2022-06-01T13:14:15");
        var when = new java.sql.Timestamp(ld.toInstant(ZoneOffset.UTC).toEpochMilli());
        var result = rh.mapToType(when, Types.TIMESTAMP);
        Assert.assertTrue(result instanceof String);
        Assert.assertEquals("2022-06-01T13:14:15", result);
    }
}

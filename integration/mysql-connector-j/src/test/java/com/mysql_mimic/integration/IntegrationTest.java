package com.mysql_mimic.integration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class IntegrationTest
{
    @Test
    public void test() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        String port = System.getenv("PORT");
        String url = String.format("jdbc:mysql://user:password@127.0.0.1:%s", port);

        Connection conn = DriverManager.getConnection(url);

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT a FROM x ORDER BY a");

        List<Integer> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getInt("a"));
        }

        List<Integer> expected = new ArrayList<>();
        expected.add(1);
        expected.add(2);
        expected.add(3);

        assertEquals(expected, result);
    }
}

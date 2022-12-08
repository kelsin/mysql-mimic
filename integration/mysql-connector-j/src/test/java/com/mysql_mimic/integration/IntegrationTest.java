package com.mysql_mimic.integration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;
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

    @Test
    public void test__krb5() throws Exception {
        // System.setProperty("sun.security.krb5.debug", "True");
        // System.setProperty("sun.security.jgss.debug", "True");
        System.setProperty("java.security.krb5.conf", System.getenv("KRB5_CONFIG"));
        System.setProperty("java.security.auth.login.config", System.getenv("JAAS_CONFIG"));

        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        String port = System.getenv("PORT");
        String url = String.format("jdbc:mysql://127.0.0.1:%s/?defaultAuthenticationPlugin=authentication_kerberos_client", port);

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

    @Test(expected = SQLException.class)
    public void test__krb5_incorrect_user() throws Exception {
        System.setProperty("java.security.krb5.conf", System.getenv("KRB5_CONFIG"));
        System.setProperty("java.security.auth.login.config", System.getenv("JAAS_CONFIG"));

        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        String user = "incorrect_user";
        String port = System.getenv("PORT");
        String url = String.format("jdbc:mysql://%s@127.0.0.1:%s/?defaultAuthenticationPlugin=authentication_kerberos_client", user, port);

        Connection conn = DriverManager.getConnection(url);

        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT a FROM x ORDER BY a");
    }
}

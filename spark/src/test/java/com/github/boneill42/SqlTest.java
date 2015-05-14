package com.github.boneill42;

import java.util.Random;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class SqlTest {

    public static void generateData() {
        Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        Session session = cluster.connect();
        session.execute("DROP KEYSPACE IF EXISTS test_keyspace");
        session.execute("CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE test_keyspace.products (id INT PRIMARY KEY, name TEXT, price FLOAT, )");
        
        Random random = new Random();
        
        for (int i = 0; i < 1000000; i++) {
            String cql = "INSERT INTO test_keyspace.products (id, name, price) values (" + i + ",'" + "name" + i + "'," + random.nextDouble() + ")";
            System.out.println(cql);
            session.execute(cql);
        }
    }

    @Test
    public void testMe() {
        // generateData();
        String[] args = {"spark://127.0.0.1:7077", "localhost"};
        //SqlRunner.main(args);
    }
}
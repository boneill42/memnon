package com.github.boneill42;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.github.boneill42.dao.ProductRowReader.ProductRowReaderFactory;
import com.github.boneill42.model.Product;

public class SqlRunner implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient SparkConf conf;
    private transient JavaSparkContext context;
    private transient JavaSQLContext sqlContext;
    private ProductRowReaderFactory productReader = new ProductRowReaderFactory();

    private SqlRunner(SparkConf conf) {
        this.conf = conf;
        context = new JavaSparkContext(conf);
        sqlContext = new JavaSQLContext(context);
    }

    private void query() {
        JavaPairRDD<Integer, Product> productsRDD = javaFunctions(context).cassandraTable("test_keyspace", "products",
                productReader).keyBy(new Function<Product, Integer>() {
            @Override
            public Integer call(Product product) throws Exception {
                return product.getId();
            }
        });
        System.out.println("Caching products...");        
        productsRDD.cache();
        System.out.println("Done.");
        
        System.out.println("Total Records = [" + productsRDD.count() + "]");
        JavaSchemaRDD schemaRDD =   sqlContext.applySchema(productsRDD.values(), Product.class);        
        sqlContext.registerRDDAsTable(schemaRDD, "products");        
        System.out.println("Querying...");        
        JavaSchemaRDD result = sqlContext.sql("SELECT id from products WHERE price < 0.50");
        System.out.println("Done.");
        for (Row row : result.collect()){
            System.out.println(row);
        }        
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster(args[0]);
        conf.set("spark.cassandra.connection.host", args[1]);

        SqlRunner app = new SqlRunner(conf);
        app.query();
    }
}
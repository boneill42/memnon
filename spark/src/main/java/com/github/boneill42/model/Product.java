package com.github.boneill42.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class Product implements Serializable {
    private static final long serialVersionUID = 1L;
    private Integer id;
    private String name;
    private Float price;

    public Product() {
    }

    public Product(Integer id, String name, Float price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getPrice() {
        return price;
    }

    public void setPrice(Float price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Product'{'id={0}, name=''{1}'', price={2}'}'", id, name, price);
    }
    
    public static List<String> columns() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("name");
        columns.add("price"); 
        return columns;
    }

}

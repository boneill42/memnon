package com.griddelta.memnon.hadoop;

import java.util.List;

import org.jruby.RubyArray;

public class ResultHelper {
    @SuppressWarnings("unchecked")
    public static List<String> getKeys(Object result) {        
        return (List<String>) ((RubyArray)result).get(0);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getValues(Object result){
        return (List<String>) ((RubyArray)result).get(1);
    }
}
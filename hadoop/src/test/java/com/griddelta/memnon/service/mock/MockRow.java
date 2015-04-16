package com.griddelta.memnon.service.mock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.AbstractGettableData;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;

public class MockRow extends AbstractGettableData implements Row{
    private final String[] data;
    private final Map<String, Integer> columns = new HashMap<String, Integer>();

    public MockRow(String[] columns, String[] data) {
        super(ProtocolVersion.V3);
        this.data = data;
        for (int i=0; i < columns.length; i++){
            this.columns.put(columns[i], i);
        }
    }

    @Override
    protected int getIndexOf(String name) {
        return columns.get(name);
    }

    @Override
    protected DataType getType(int i) {
        return null;
    }

    @Override
    protected String getName(int i) {
        return null;
    }

    @Override
    protected ByteBuffer getValue(int i) {
        return null;
    }
    
    @Override
    public String getString(int i) {
        return data[i];
    }
    
    @Override
    public String getString(String column) {
        return data[this.getIndexOf(column)];
    }

	public ColumnDefinitions getColumnDefinitions() {
		return null;
	}
}

package com.lb.spring.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public interface HbaseOperations {
		
	Table getTable(String tableName) throws IOException;
	
	<T> T execute(String tableName, TableCallback<T> action);
	
	<T> T find(String tableName, String family, final ResultsExtractor<T> action);
	
	<T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action);
	
	<T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action);
	
	<T> List<T> find(String tableName, String family, final RowMapper<T> action);
	
	<T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action);
	
	<T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action);
	
	<T> T get(String tableName, String rowKey, final RowMapper<T> mapper);
	
	<T> T get(String tableName, final String rowKey, final String familyName, final RowMapper<T> mapper);
	
	<T> T get(String tableName, final String rowKey, final String familyName, final String qualifier, final RowMapper<T> mapper);
	
	void put(String tableName, final String rowKey, final String familyName, final String qualifier, final byte[] data);
	
	void delete(String tableName, final String rowKey, final String familyName);
	
	void delete(String tableName, final String rowKey, final String familyName, final String qualifier);
	
}
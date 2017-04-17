package com.lb.spring.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

public interface HbaseAnnotation extends HbaseOperations {
		
	<T> T get(String tableName, String rowkey, Class<?> clazz);
	
	<T> T get(String tableName, Get get, Class<?> clazz);
	
	<T> void put(String tableName, T t);
	
	<T> void put(String tableName, List<T> list);
	
	<T> void delete(String tableName, String rowkey);
	
	<T> void delete(String tableName, T t);
	
	<T> void delete(String tableName, Delete delete);

	<T> List<T> find(String tableName, final Scan scan, Class<?> clazz);
	
}
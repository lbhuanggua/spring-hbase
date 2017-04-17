package com.lb.spring.hbase;

import org.apache.hadoop.hbase.client.ResultScanner;

public interface ResultsExtractor<T> {

	T extractData(ResultScanner results) throws Exception;
	
}

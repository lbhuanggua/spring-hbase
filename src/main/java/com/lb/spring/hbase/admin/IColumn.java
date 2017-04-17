package com.lb.spring.hbase.admin;

public interface IColumn {
	String rowkey();

	String family();

	String qualifier();

	String value();

	long timestamp();
}

package com.lb.spring.hbase.admin;

import java.util.ArrayList;
import java.util.List;

public class ColumnList {

	public static abstract class AbstractColumn {
		String family, qualifier;

		AbstractColumn(String family, String qualifier) {
			this.family = family;
			this.qualifier = qualifier;
		}

		public String getFamily() {
			return family;
		}

		public String getQualifier() {
			return qualifier;
		}

	}

	public static class Column extends AbstractColumn {
		String rowkey;
		String value;
		long ts = -1;

		Column(String rowkey, String family, String qualifier, long ts,
				String value) {
			super(family, qualifier);
			this.rowkey = rowkey;
			this.value = value;
			this.ts = ts;
		}

		public String getRowkey() {
			return rowkey;
		}

		public String getValue() {
			return value;
		}

		public long getTs() {
			return ts;
		}
	}

	public static class Counter extends AbstractColumn {
		long incr = 0;

		Counter(String family, String qualifier, long incr) {
			super(family, qualifier);
			this.incr = incr;
		}

		public long getIncrement() {
			return incr;
		}
	}

	private ArrayList<Column> columns;
	private ArrayList<Counter> counters;

	private ArrayList<Column> columns() {
		if (this.columns == null) {
			this.columns = new ArrayList<Column>();
		}
		return this.columns;
	}

	private ArrayList<Counter> counters() {
		if (this.counters == null) {
			this.counters = new ArrayList<Counter>();
		}
		return this.counters;
	}

	public ColumnList addColumn(String rowkey, String family, String qualifier,
			long ts, String value) {
		columns().add(new Column(rowkey, family, qualifier, ts, value));
		return this;
	}

	public ColumnList addColumn(String rowkey, String family, String qualifier,
			String value) {
		columns().add(new Column(rowkey, family, qualifier, -1, value));
		return this;
	}

	public ColumnList addColumn(IColumn column) {
		return this.addColumn(column.rowkey(), column.family(),
				column.qualifier(), column.timestamp(), column.value());
	}

	public ColumnList addCounter(String family, String qualifier, long incr) {
		counters().add(new Counter(family, qualifier, incr));
		return this;
	}

	public boolean hasColumns() {
		return this.columns != null;
	}

	public boolean hasCounters() {
		return this.counters != null;
	}

	public List<Column> getColumns() {
		return this.columns;
	}

	public List<Counter> getCounters() {
		return this.counters;
	}

}

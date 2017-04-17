package com.lb.spring.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.util.Assert;

import com.lb.spring.hbase.annotation.HbaseColumn;
import com.lb.spring.hbase.annotation.HbaseMap;
import com.lb.spring.hbase.annotation.HbaseRowKey;

public class HbaseTemplate extends HbaseAccessor implements HbaseAnnotation {

	public HbaseTemplate() {
	}

	public HbaseTemplate(Configuration configuration) {
		super.setConfiguration(configuration);
		super.afterPropertiesSet();
	}

	public HbaseTemplate(String remoteUser) {
		super.setRemoteUser(remoteUser);
		super.afterPropertiesSet();
	}

	public HbaseTemplate(Configuration configuration, String remoteUser) {
		super.setConfiguration(configuration);
		super.setRemoteUser(remoteUser);
		super.afterPropertiesSet();
	}

	@Override
	public Table getTable(String tableName) throws IOException {
		return getConnection().getTable(TableName.valueOf(tableName));
	}

	@Override
	public <T> T execute(String tableName, TableCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");
		Table table = null;
		try {
			table = getTable(tableName);
			return action.doInTable(table);
		} catch (Throwable th) {
			if (th instanceof Error) {
				throw ((Error) th);
			}
			if (th instanceof RuntimeException) {
				throw ((RuntimeException) th);
			}
			throw new HbaseSystemException((Exception) th);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public <T> T get(String tableName, String rowKey, Class<?> clazz) {
		Assert.hasLength(rowKey, "this String argument must have length; it must not be null or empty");
		Get get = new Get(rowKey.getBytes(getCharset()));
		return get(tableName, get, clazz);
	}
	
	@Override
	public <T> T get(String tableName, Get get, Class<?> clazz) {
		Assert.notNull(get, "this argument is required; it must not be null");
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table table) throws Throwable {
				Result result = table.get(get);
				return get(result, clazz);
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	public <T> T get(Result result, Class<?> clazz) throws InstantiationException, IllegalAccessException, ParseException{
		if(result.isEmpty()) {
			return null;
		}
		T t = (T) clazz.newInstance();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			Class<?> type = field.getType();
			byte[] fieldValue = new byte[0];
			HbaseRowKey hRowKey = field.getDeclaredAnnotation(HbaseRowKey.class);
			if (hRowKey != null) {
				fieldValue = result.getRow();
			}
			HbaseColumn hColumn = field.getDeclaredAnnotation(HbaseColumn.class);
			if (hColumn != null && hColumn.read()) {
				fieldValue = result.getValue(hColumn.family().getBytes(getCharset()), hColumn.qualifier().getBytes(getCharset()));
			}
			if (fieldValue != null && (hColumn != null || hRowKey != null)){
				if (type.getName().equals("java.lang.Boolean")) {
					field.set(t, Boolean.parseBoolean(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Byte")) {
					field.set(t, Byte.parseByte(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Short")) {
					field.set(t, Short.parseShort(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Integer")) {
					field.set(t, Integer.parseInt(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Long")) {
					field.set(t, Long.parseLong(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Float")) {
					field.set(t, Float.parseFloat(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.Double")) {
					field.set(t, Double.parseDouble(new String(fieldValue)));
				} else if (type.getName().equals("java.lang.String")) {
					field.set(t, new String(fieldValue));
				} else if (type.getName().equals("java.time.LocalDate")) {
					field.set(t, LocalDate.parse(new String(fieldValue)));
				} else if (type.getName().equals("java.time.LocalTime")) {
					field.set(t, LocalTime.parse(new String(fieldValue)));
				} else if (type.getName().equals("java.time.LocalDateTime")) {
					field.set(t, LocalDateTime.parse(new String(fieldValue)));
				} else if (type.getName().equals("java.time.ZonedDateTime")) {
					field.set(t, ZonedDateTime.parse(new String(fieldValue)));
				} else {
					field.set(t, fieldValue);
				}
			}
		}
		return t;
	}
	
	@Override
	public <T> void put(String tableName, T t) {
		execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table table) throws Throwable {
				byte[] rowKey = getRowKey(t);
				Put put = new Put(rowKey);
				Field[] fields = t.getClass().getDeclaredFields();
				for (Field field : fields) {
					field.setAccessible(true);
					putColumn(put, field, t);
				}
				table.put(put);
				return null;
			}
		});
	}
	
	<T> void putColumn(Put put, Field field, T t) throws IllegalArgumentException, IllegalAccessException{
		HbaseColumn hColumn = field.getDeclaredAnnotation(HbaseColumn.class);
		if (hColumn != null && hColumn.write()) {
			Object val = field.get(t);
			if (val == null) {
				val = new String("");
			}
			put.addColumn(hColumn.family().getBytes(getCharset()), hColumn.qualifier().getBytes(getCharset()), val.toString().getBytes(getCharset()));
		}
		HbaseMap hMap = field.getDeclaredAnnotation(HbaseMap.class);
		if (hMap != null && hMap.write()) {
			Class<?> type = field.getType();
			if (type.getName().equals("java.util.Map")) {
				Object val = field.get(t);
				if(val != null) {
					@SuppressWarnings("unchecked")
					Map<Object, Object> map = (HashMap<Object, Object>) val;
					for (Map.Entry<Object, Object> entry : map.entrySet()) {
						 put.addColumn(hMap.family().getBytes(getCharset()), entry.getKey().toString().getBytes(getCharset()), entry.getValue().toString().getBytes(getCharset()));
					}
				}
			} 
		}
	}
	
	<T> byte[] getRowKey(T t) throws IllegalArgumentException, IllegalAccessException{
		Field[] fields = t.getClass().getDeclaredFields();
		Assert.notNull(fields, "field must not be null");
		int flag = 0;
		byte[] value = null;
		for (Field field : fields) {
			field.setAccessible(true);
			HbaseRowKey hRowKey = field.getDeclaredAnnotation(HbaseRowKey.class);
			if(hRowKey != null) {
				value = field.get(t).toString().getBytes(getCharset());
				flag++;
			}
		}
		Assert.state(flag == 1, "field has not rowkey or has more one");
		return value;
	}

	@Override
	public <T> void put(String tableName, List<T> list) {
		Assert.notEmpty(list, "this collection must not be empty: it must contain at least 1 element");
		execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table table) throws Throwable {
				List<Put> puts = new ArrayList<Put>();
				for (T t : list) {
					byte[] rowKey = getRowKey(t);
					Put put = new Put(rowKey);
					Field[] fields = t.getClass().getDeclaredFields();
					for (Field field : fields) {
						field.setAccessible(true);
						putColumn(put, field, t);
					}
					puts.add(put);
				}
				table.put(puts);
				return null;
			}
		});
	}

	@Override
	public <T> void delete(String tableName, String rowKey) {
		Assert.hasLength(rowKey, "this String argument must have length; it must not be null or empty");
		Delete delete = new Delete(rowKey.getBytes(getCharset()));
		delete(tableName, delete);
	}

	@Override
	public <T> void delete(String tableName, T t) {
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table table) throws Throwable {
				byte[] rowKey = getRowKey(t);
				Assert.notNull(rowKey, "this argument is required; it must not be null");
				Delete delete = new Delete(rowKey);
				table.delete(delete);
				return null;
			}
		});
	}
	
	@Override
	public <T> void delete(String tableName, Delete delete) {
		Assert.notNull(delete, "this argument is required; it must not be null");
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table table) throws Throwable {
				table.delete(delete);
				return null;
			}
		});
	}
	
	@Override
	public <T> List<T> find(String tableName, Scan scan, Class<?> clazz) {
		return this.execute(tableName, new TableCallback<List<T>>(){
			@Override
			public List<T> doInTable(Table table) throws Throwable {
				ResultScanner scanner = table.getScanner(scan);
				List<T> ts = new ArrayList<T>();
				try {
					for (Result result : scanner) {
						T t = get(result, clazz);
						ts.add(t);
					}
				} finally {
					scanner.close();
				}
				return ts;
			}
		});
	}
	
	@Override
	public <T> T find(String tableName, String family, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table table) throws Throwable {
				ResultScanner scanner = table.getScanner(scan);
				try {
					return action.extractData(scanner);
				} finally {
					scanner.close();
				}
			}
		});
	}

	@Override
	public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
		return find(tableName, scan, new RowMapperResultsExtractor<T>(action));
	}

	@Override
	public <T> T get(String tableName, String rowKey, final RowMapper<T> mapper) {
		return get(tableName, rowKey, null, null, mapper);
	}
	
	@Override
	public <T> T get(String tableName, String rowKey, String familyName, final RowMapper<T> mapper) {
		return get(tableName, rowKey, familyName, null, mapper);
	}

	@Override
	public <T> T get(String tableName, final String rowKey, final String familyName, final String qualifier, final RowMapper<T> mapper) {
		return execute(tableName, new TableCallback<T>() {
			@Override
			public T doInTable(Table table) throws Throwable {
				Get get = new Get(rowKey.getBytes(getCharset()));
				if (familyName != null) {
					byte[] family = familyName.getBytes(getCharset());
					if (qualifier != null) {
						get.addColumn(family, qualifier.getBytes(getCharset()));
					} else {
						get.addFamily(family);
					}
				}
				Result result = table.get(get);
				return mapper.mapRow(result, 0);
			}
		});
	}

	@Override
	public void put(String tableName, final String rowKey, final String familyName, final String qualifier, final byte[] value) {
		Assert.hasLength(rowKey, "this String argument must have length; it must not be null or empty");
		Assert.hasLength(familyName, "this String argument must have length; it must not be null or empty");
		Assert.hasLength(qualifier, "this String argument must have length; it must not be null or empty");
		Assert.notNull(value, "this argument is required; it must not be null");
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table table) throws Throwable {
				Put put = new Put(rowKey.getBytes(getCharset())).addColumn(familyName.getBytes(getCharset()), qualifier.getBytes(getCharset()), value);
				table.put(put);
				return null;
			}
		});
	}

	@Override
	public void delete(String tableName, final String rowKey, final String familyName) {
		delete(tableName, rowKey, familyName, null);
	}
	
	@Override
	public void delete(String tableName, final String rowKey, final String familyName, final String qualifier) {
		Assert.hasLength(rowKey, "this String argument must have length; it must not be null or empty");
		Assert.hasLength(familyName, "this String argument must have length; it must not be null or empty");
		execute(tableName, new TableCallback<Object>() {
			@Override
			public Object doInTable(Table table) throws Throwable {
				Delete delete = new Delete(rowKey.getBytes(getCharset()));
				byte[] family = familyName.getBytes(getCharset());
				if (qualifier != null) {
					delete.addColumn(family, qualifier.getBytes(getCharset()));
				} else {
					delete.addFamily(family);
				}
				table.delete(delete);
				return null;
			}
		});
	}	
	
}
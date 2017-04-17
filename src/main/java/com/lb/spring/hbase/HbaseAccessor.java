package com.lb.spring.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

public abstract class HbaseAccessor implements InitializingBean, DisposableBean{
	
	protected static final Logger log = LoggerFactory.getLogger(HbaseAccessor.class);
	 
	private String encoding;
	private Connection connection;
	private Configuration configuration;
	private Properties properties;
	private String remoteUser;

	@Override
	public void afterPropertiesSet() {
		configuration = (configuration != null ? HBaseConfiguration.create(configuration) : HBaseConfiguration.create());
		if (properties != null) {
			Enumeration<?> props = properties.propertyNames();
			while (props.hasMoreElements()) {
				String key = props.nextElement().toString();
				configuration.set(key, properties.getProperty(key));
			}
		}
	}
	
	@Override
	public void destroy() {
		if (connection != null && !connection.isClosed()) {
			try {
				connection.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	public Connection getConnection() {
		if (connection == null) {
			try {
				if(StringUtils.hasText(remoteUser)) {
					User user = User.create(UserGroupInformation.createRemoteUser(remoteUser));
					connection = ConnectionFactory.createConnection(configuration, user);
				} else {
					connection = ConnectionFactory.createConnection(configuration);
				}
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
		}
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public String getRemoteUser() {
		return remoteUser;
	}

	public void setRemoteUser(String remoteUser) {
		this.remoteUser = remoteUser;
	}

	public Charset getCharset() {
		return ((encoding!=null && !encoding.equals("")) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
	}

}
package com.lb.spring.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HbaseMap {
	
	/**
	 * 列族
	 * @return
	 */
	String family() default "unknown";
	/**
	 * 读 暂时未实现
	 * @return
	 */
	boolean read() default true;
	/**
	 * 写
	 * @return
	 */
	boolean write() default true;
	
}

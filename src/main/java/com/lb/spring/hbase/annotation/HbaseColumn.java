package com.lb.spring.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HbaseColumn {
	
	/**
	 * 列族
	 * @return
	 */
	String family() default "unknown";
	/**
	 * 列
	 * @return
	 */
	String qualifier() default "unknown";
	/**
	 * 读
	 * @return
	 */
	boolean read() default true;
	/**
	 * 写
	 * @return
	 */
	boolean write() default true;
	
}

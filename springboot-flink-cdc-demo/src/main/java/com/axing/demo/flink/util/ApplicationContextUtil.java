package com.axing.demo.flink.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class ApplicationContextUtil implements ApplicationContextAware, Serializable {
 
	/**
	 * 上下文
	 */
	private static ApplicationContext context;
 
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            context = applicationContext;
	}
 
	public static ApplicationContext getApplicationContext() {
            return context;
	}
 
	public static <T> T getBean(Class<T> beanClass) {
            return context.getBean(beanClass);
	}
}
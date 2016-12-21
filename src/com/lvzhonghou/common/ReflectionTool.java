package com.lvzhonghou.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月24日 下午3:46:32 
 * @version v1。0
 */
public class ReflectionTool {
    public static Object getProperty(Object owner, String fieldName) {
	Class ownerClass = owner.getClass();
	Field field;
	Object property = null;
	try {
	    field = ownerClass.getField(fieldName);
	    
	    property = field.get(owner);
	} catch (NoSuchFieldException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (SecurityException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IllegalArgumentException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	
	return property;
    }
    
    public static String[] getPropertyNames(Class clazz) {
	Field[] fields = clazz.getDeclaredFields();
	// String[] properties = new String[fields.length];
	List<String> noStaticPropers = new ArrayList<String>();
	
	for(int i = 0; i < fields.length; i++) {
	    if(!Modifier.isStatic(fields[i].getModifiers())) {
		noStaticPropers.add(fields[i].getName());
	    }
	}
	
	String[] properties = new String[noStaticPropers.size()];
	noStaticPropers.toArray(properties);
	return properties;
    }
}

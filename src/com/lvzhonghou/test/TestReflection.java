package com.lvzhonghou.test;

import java.lang.reflect.Field;

import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.common.MapCostStatistics;
import com.lvzhonghou.common.ReflectionTool;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月25日 下午1:28:07 
 * @version v1。0
 */
public class TestReflection {

    /** 
     * @Description 
     * @param args  
     * @throws SecurityException 
     * @throws NoSuchFieldException 
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
    public static void main(String[] args) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
	// TODO Auto-generated method stub
	Class clazz = MapTaskParameters.class;
	
	String[] propertiesName = ReflectionTool.getPropertyNames(clazz);
	for(String str : propertiesName) {
	    System.out.println(str);
	}
	
	Class clazz1 = MapCostStatistics.class;
	String[] propertiesName1 = ReflectionTool.getPropertyNames(clazz1);
	for(String str : propertiesName1)
	    System.out.println(str);
	
	Field field = clazz.getDeclaredField("pIsCombine");
	String type = field.getType().toString();
	if(type.endsWith("Double") || type.endsWith("double")) {
	    System.out.println("this type is double" );
	} else if(type.endsWith("boolean") || type.endsWith("Boolean")) {
	    System.out.println("this type is boolean");
	}
	
	MapTaskParameters parameters = new MapTaskParameters();
	Class clazz2 = parameters.getClass();
	Field field1 = clazz2.getDeclaredField("pSplitSize");
	field1.set(parameters, 1.0);
	double ret = field1.getDouble(parameters);
	System.out.println(ret);
    }

}

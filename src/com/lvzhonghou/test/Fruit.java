package com.lvzhonghou.test;

import java.lang.reflect.Field;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月24日 下午2:15:03 
 * @version v1。0
 */
public class Fruit {
    String name;
    private final static int price = 10;
    
    public static Object getProperty(Object owner, String fieldName) {
	Class ownerClass = owner.getClass();
	Field field;
	Object property = null;
	try {
	    field = ownerClass.getField(fieldName);
	    property = field.get(owner);
	} catch (NoSuchFieldException | SecurityException e) {
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
    
    public static void main(String[] args) {
	Fruit fruit = new Fruit();
	fruit.name = "apple";
	
	Fruits fruits = new Fruits();
	fruits.apple = 10;
	fruits.banana = 20;
	
	Object object = getProperty(fruits, fruit.name);
	System.out.println(object);
	
	System.out.println("the price of the " + fruit.name + " is " + fruit.price);
    }
}

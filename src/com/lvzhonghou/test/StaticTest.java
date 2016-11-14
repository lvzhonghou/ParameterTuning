package com.lvzhonghou.test;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月31日 下午1:29:57 
 * @version v1。0
 */
public class StaticTest {
    public static int getNum(int i) {
	return i;
    }
    
    public String getStr(String str) {
	return str;
    }

    /** 
     * @Description 
     * @param args  
     */
    public static void main(String[] args) {
	StaticTest staticTest = new StaticTest();
	int num = staticTest.getNum(10);
	System.out.println("the num is " + num);

    }

}

package com.lvzhonghou.test;

import com.lvzhonghou.StatisticsEstimate.ParaStatisLink;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月25日 下午7:02:44 
 * @version v1。0
 */
public class TestParaStatisLink {

    /** 
     * @Description 
     * @param args  
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	ParaStatisLink psLink = new ParaStatisLink();
	String[] strs = new String[]{"lv", "zhonghou", "tan", "meili"};
	String str = "lv";
	boolean isCon = psLink.isContain(str, strs);
	if(isCon) {
	    System.out.println("strs contain the " + str);
	} else {
	    System.out.println("strs don't contain the " + str);
	}
	
	String[] strs1 = new String[]{"lv1", "zhonghou1", "tan1"};
	String[] strs2 = new String[]{"lv", "tan", "mei", "zhonghou"};
	String[] commonStrs = psLink.commonStrs(strs1, strs2);
	for(String str1 : commonStrs)
	    System.out.println(str1);
	

    }

}

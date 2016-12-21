package com.lvzhonghou.test;

import java.util.ArrayList;
import java.util.Map;

import com.lvzhonghou.StatisticsEstimate.ParaStatisLink;
import com.lvzhonghou.StatisticsEstimate.Parameter;
import com.lvzhonghou.StatisticsEstimate.Statistic;

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
	String[] strs2 = new String[]{"lv1", "tan", "mei", "zhonghou"};
	String[] commonStrs = psLink.commonStrs(strs1, strs2);
	if(commonStrs == null) {
	    System.out.println("the commonStrs is null.");
	    return;
	}
	    
	for(String str1 : commonStrs)
	    System.out.println(str1);
	
	
	// show the paraStatis Link
	psLink.init();
	Map<Parameter, ArrayList<Statistic>> map = psLink.paraStatisMap;
	for(Map.Entry<Parameter, ArrayList<Statistic>> entry : map.entrySet()) {
	    Parameter paraKey = entry.getKey();
	    ArrayList<Statistic> statisVal = entry.getValue();
	    System.out.println("parameter: ");
	    System.out.println(paraKey.paraName + " " + paraKey.mapOrReduce + " " + paraKey.isActive + " " + paraKey.isTradeOff);
	    System.out.println("Statistics: ");
	    if(statisVal == null || statisVal.size() == 0) {
		System.out.println("null");
	    } else {
		for(Statistic statis : statisVal) {
		    System.out.println(statis.name + " " + statis.mapOrReduce + " " + statis.statType + " " + statis.isActive);
		}
	    }
	}
	
	ArrayList<Statistic> inactive = (ArrayList<Statistic>)psLink.inactiveStatis;
	System.out.println("Show the inactive statistics!");
	for(Statistic statis : inactive) {
	    System.out.println(statis.name + " " + statis.mapOrReduce + " " + statis.statType + statis.isActive);
	}
	

    }

}

package com.lvzhonghou.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.lvzhonghou.Prophet.MapOut;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月15日 下午2:21:16 
 * @version v1。0
 */
public class MapOutSortTest {
    
    /** 
     * @Description 
     * @param args  
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	List<MapOut> mapOuts = new ArrayList<MapOut>();
	MapOut mapout1 = new MapOut(16, 1, 1, 1, 1, 1);
	MapOut mapout2 = new MapOut(16, 2, 2, 1, 1, 2);
	MapOut mapout3 = new MapOut(16, 4, 4, 1, 1, 4);
	MapOut mapout4 = new MapOut(16, 3, 3, 1, 1, 3);
	
	mapOuts.add(mapout1);
	mapOuts.add(mapout2);
	mapOuts.add(mapout3);
	mapOuts.add(mapout4);
	
	Collections.sort(mapOuts);
	
	for(MapOut mapout : mapOuts) {
	    System.out.println(mapout.getNodeId() + " " + mapout.getMapCost());
	}
	
    }

}

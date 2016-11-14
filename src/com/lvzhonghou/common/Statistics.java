package com.lvzhonghou.common;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.Prophet.ReduceTaskParameters;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年3月19日 下午2:56:17 
 * @version v1。0
 */
public class Statistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888093L;
    
    public boolean isEmpty = false;
    public String hostName;

    // map and reduce statistics
    public MapStatistics mapStatistics = null;
    public ReduceStatistics reduceStatistics = null;
    
    // map and reduce parameters
    public MapTaskParameters mapParameters = null;
    public ReduceTaskParameters reduceParameters = null;
    
    /**
     * Description 
     * @param o
     * @return 
     * @see java.lang.Comparable#compareTo(java.lang.Object) 
     */ 
    	
}

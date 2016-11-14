package com.lvzhonghou.test;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月26日 下午5:43:30 
 * @version v1。0
 */
public class TestArgs {

    /** 
     * @Description 
     * @param args  
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	String jobIds = "";
	for(int i = 0; i < args.length; i++) {
	    jobIds += args[i] + " ";
	}
	System.out.println(jobIds);
	String[] jobIdArr = jobIds.split(" ");
	for(String jobId : jobIdArr)
	    System.out.println(jobId);
	
	double d = 10.0;
	int i = (int)d;
	System.out.println(i);
	
    }

}

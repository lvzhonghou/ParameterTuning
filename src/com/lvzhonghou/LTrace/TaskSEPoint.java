package com.lvzhonghou.LTrace;

import java.util.Date;

import com.lvzhonghou.common.MapOrReduce;

/** 
 * @Description this class records the start and end of the map and reduce tasks
 * @author zhonghou.lzh
 * @date 2016年7月21日 下午4:27:45 
 * @version v1。0
 */
public class TaskSEPoint implements Comparable<TaskSEPoint> {
    Date start; // the start of the task
    Date end;   // the end of the task
    MapOrReduce mapOrReduce;  // the task type
    long val;
    
    public TaskSEPoint() {
	
    }
    
    public TaskSEPoint(TaskSEPoint task, int se) {
	this.start = task.start;
	this.end = task.end;
	this.mapOrReduce = mapOrReduce;
	
	// when se is 0, the val is start, when se is 1, the val is end
	if(se == 0)
	    val = start.getTime();
	else if(se == 1)
	    val = end.getTime();
    }
    
    /**
     * Description 
     * @param o
     * @return 
     * @see java.lang.Comparable#compareTo(java.lang.Object) 
     */ 
    	
    @Override
    public int compareTo(TaskSEPoint taskSEPoint) {
        // TODO Auto-generated method stub
	if(this.val >= taskSEPoint.val)
	    return 1;
	else
	    return -1;	    
    }
    
    
}

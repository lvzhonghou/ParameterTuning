package com.lvzhonghou.Prophet;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��7��13�� ����9:51:28 
 * @version v1��0
 */
public class ClockSegment {
    double recs;
    double size;
    double writeClock;

    public ClockSegment() {

    }

    public ClockSegment(double recs, double size, double writeClock) {
	this.recs = recs;
	this.size = size;
	this.writeClock = writeClock;
    }
}


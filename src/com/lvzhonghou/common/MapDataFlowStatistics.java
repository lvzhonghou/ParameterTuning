package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月17日 下午8:47:51 
 * @version v1。0
 */
public class MapDataFlowStatistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888095L;
    
    public double dsInputPairWidth;  // map函数一次处理的数据大小，输入文件一行的大小
    
    public double dsMapRecsSelect;  // 一次map函数能产生的k、v个数
    public double dsMapOutRecWidth;  //缓冲区中存储一个k、v的宽度,包括元数据
    
    public double dsCombineRecsSelect; //combine后k、v的数量转化率
    public double dsSpillOutRecWidth; //spill输出文件k、v的宽度
    
    public double dsMergeCombineRecsSelect; //merge阶段combine时的k、v的数量转化率
}

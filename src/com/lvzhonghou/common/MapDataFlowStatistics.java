package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��1��17�� ����8:47:51 
 * @version v1��0
 */
public class MapDataFlowStatistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888095L;
    
    public double dsInputPairWidth;  // map����һ�δ�������ݴ�С�������ļ�һ�еĴ�С
    
    public double dsMapRecsSelect;  // һ��map�����ܲ�����k��v����
    public double dsMapOutRecWidth;  //�������д洢һ��k��v�Ŀ��,����Ԫ����
    
    public double dsCombineRecsSelect; //combine��k��v������ת����
    public double dsSpillOutRecWidth; //spill����ļ�k��v�Ŀ��
    
    public double dsMergeCombineRecsSelect; //merge�׶�combineʱ��k��v������ת����
}

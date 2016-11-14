package com.lvzhonghou.common;
import java.io.Serializable;
import java.util.ArrayList;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��1��17�� ����5:14:04 
 * @version v1��0
 */
public class MapCostStatistics implements Serializable {
    private static final long serialVersionUID = -5240152692961888094L;
    
    public double csScheduleCost;  //map task�ĵ��ȿ���
    
    public double csInitialCost;   //��ʼ������
    public double hdfsReadRate = 100;   //hdfs��ȡ�ٶ�  MB/Sec
    public double ioReadRate = 130;   //����io��ȡ�ٶ�
    public double csReadCost;  //�����ݿ���
    public double csMapCost;  //ִ��map��������(û��spill�߳�ִ���µ�map����)
    public double csMapCostWithSpill;  //ִ��map��������(��spill�߳�ִ�е�ͬʱ��map�����Ŀ���)
    
    public double csSortCost;  //������
    public double csCombineCost;  //combine����
    public double csCombineReadCost;    //���������Ŀ������Ի����������ݽ��з����л��Ŀ���
    public double csCombineWriteCost;  //���л�д�Ŀ���
    
    public double csMergeReadCost;   //merge��ȡ�ļ��������л�����
    public double csMergeCombineCost;   //merge combine����
    public double csMergeWriteCost;   //merge���л���д�ļ�

}

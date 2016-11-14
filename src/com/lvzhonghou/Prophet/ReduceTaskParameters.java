package com.lvzhonghou.Prophet;
import java.io.Serializable;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��3��4�� ����7:45:51 
 * @version v1��0
 */
public class ReduceTaskParameters implements Serializable {
    /** @Fields serialVersionUID: */
    private static final long serialVersionUID = -8516350778084476378L;
    
    public double pTotalMemorySize;  //memoryLimit 
   // double pMaxInMemCopyUse;  //�ڴ����ڴ洢mapoutput�ı���(0.7)
    public double pSingleShuffleMemoryLimitPercent;  //�ڴ�����������mapoutput�ı��� (0.25)
    public double pMergeThresholdPercent;  //mergeThreshold / memoryLimit (0.9)
    public double pSortFactor;  //�����ļ����κϲ����ļ���(2 * pSortFactor - 1)
    public double pCopyThread; //�����̵߳ĸ���
    public double pReduceInBufPerc;  //reduceʱ���ڴ��й����mapoutput�Ŀռ����
    public double pNumReduce;  //reduceTask�ĸ���
    public boolean pHaveCombiner = false; //memToDisk�Ƿ���Ҫ����combine����
}
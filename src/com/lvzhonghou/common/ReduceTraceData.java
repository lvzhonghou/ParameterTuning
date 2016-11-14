package com.lvzhonghou.common;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��2��28�� ����8:21:55 
 * @version v1��0
 */
public class ReduceTraceData {
   public long initialCost;   //��ʼ��ʱ��
   
   public int memoryLimit;   //memory capacity for shuffle
   public int maxSingleShuffleLimit;  //�ڴ������ɵ���mapoutput�����ֵ
   public int mergeThreshold;   //�ڴ�ϲ��ķ�ֵ
   public int ioSortFactor;    //�����ļ��ϲ����ļ���
   public double maxInMemCopyUse;   //�ڴ����ڴ洢mapoutput�ı���
   public double singleShuffleMemoryLimitPercent;   //�ڴ�����������mapoutput�ı���
   public List<ShuffleInfo> shuffles;  //����shuffles����Ϣ
   public List<MemMergeInShuffle> memMergesInShu;
   public List<DiskMergeInShuffle> diskMergesInShu;
   public double reduceInBufPerc;   //reduceʱ���ڴ��й����mapoutput�Ŀռ����
   public int maxInMemReduce;   //reduceʱ���ڴ��й����mapoutput�Ŀռ�
   public FinalMerge finalMerge;  //shuffle��֮��finalMerge
   public long reduceCost;   //reduce����  
   public long hdfsWriteCost;   //hdfsд����
   public long numsReduce;    //reduceִ�д���
   public long reduceOutSize;//hdfsд�����ݴ�С
}


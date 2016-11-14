package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��7��12�� ����3:47:30 
 * @version v1��0
 */
public class FinalMerge {
   public int numMemMapOutputs;  //�ڴ���mapoutputs����  
   public int numDiskMapOutputs; //�������ļ�����
   public boolean isFinalMerge;   //�Ƿ����һ�ֺϲ�
   public long memToDiskSize;  //���ڴ���memToDiskSize��С�ϲ��������ļ���
   public boolean isCleanMemToDisk = true;  //�Ƿ���Ҫ���ڴ�Ĳ������ݺϲ�д�������,��ղ�������
   public boolean isFirstInmeMerge = true;  //�Ƿ���Ҫ���е�һ���м�ϲ�
   public boolean haveCombiner; //�Ƿ���combiner
   public long memToDiskMergeCost;  //���ڴ��кϲ��Ŀ��������л��뷴���л��Ŀ���
   public long memToDiskWriteCost;   //���ڴ�ϲ���д����̵Ŀ���
   
   public long immeFirstImmeMergeCost = 0; //��һ�κϲ�
   public long immeFirstImmeWriteCost = 0;
   public long immeFirstImmeSize = 0;
    
   public long immeNotFirstImmeMergeCost;
   public long immeNotFirstImmeWriteCost;
   public long immeNotFirstImmeSize;
    
   public long finalMemSize;   //���ϲ��ڴ����ݵĴ�С
   public long finalDiskSize;   //���ϲ��������ݵĴ�С
   public long finalMergeSize;  //���ϲ��ܵ����ݴ�С��finalMergeSize = finalDiskSize + finalMemSize
   public long finalMergeCost;   //���ϲ��Ŀ���
}

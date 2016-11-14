package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��1��16�� ����10:52:14 
 * @version v1��0
 */
public class SpillTrace {
   public long mapToSpill; // map��ʼִ�е�spill��ʼ
   public long mainThreadSleep; // ���߳�����ʱ��
   public long spillTime; // ����spillʱ��
   public long sortTime; // ����ʱ��
   public long combineAndWrite; // combineAndWriteʱ��
   public long readTime = 0; // readʱ��
   public long writeTime = 0; // writeʱ��
   public int spillOutFile; // spill����ļ���С
   public int kvNums; // ��ǰspill����Ҫ�����key��value��Ŀ
   public int partitionNums;  //partition�ĸ�����ÿ��partition������һ��combine
   public boolean haveCombiner = true;
   public int combineOutRecs = 0; // combine�������records��
}
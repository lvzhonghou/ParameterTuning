package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��7��12�� ����3:58:46 
 * @version v1��0
 */
public class MergeTrace {
    public long mergeSize; //�ϲ��׶ε��ļ���С
    public long imediateMergeCost;  //�м�ϲ��Ŀ���
    public long mergeCost;  //merge����
    public long mergeReadCost;  //mersge read����
    public long mergeWriteCost;  //merge write����
    public boolean haveCombineWithMerge;  //merge�׶��Ƿ����combine
    public long mergeOutRecs;  //merge���kv��
}

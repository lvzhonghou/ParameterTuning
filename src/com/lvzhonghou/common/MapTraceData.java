package com.lvzhonghou.common;
import java.util.ArrayList;
import java.util.List;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��1��16�� ����10:57:09 
 * @version v1��0
 */
public class MapTraceData {
   public long initialCost; // ��ʼ������
   public int sortMB; // sortMB��С
   public int sortLimit; // sortLimit��С
   public int setUpCost; // setup����
   public ArrayList<SpillTrace> spillList = new ArrayList<SpillTrace>();
   public long mapCost; // map����ִ�еĿ���
   public int maps; // map����ѭ��ִ�еĴ���
   public int cleanUp; // cleanup����
   public long inputFileSize; // mapTask����������ļ���С
   public long readAndMapCost; // ��ȡsplit���ݺ�ִ��map�Ŀ���
   public int sortFactor; // ָ����sortFactor
   public int numSpillForComb; // mergeʱ����combine��spill������ֵ
   public List<MergeTrace> mergeList = new ArrayList<MergeTrace>();
   public long mapTaskCost; // ����map����Ŀ���
   public int mapOutBytes; // map�����������������ֽ�����
   public int mapOutRecs; // map�������е��������ļ�¼��
    
   public int mapOutRecsWithSpill; //��spill��ͬʱ��map��������ļ�¼��
   public long mapCostWithSpill;  //��spill��ͬʱ��map����ִ�е��ܿ���
}


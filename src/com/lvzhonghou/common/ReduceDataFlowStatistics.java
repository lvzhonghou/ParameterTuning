package com.lvzhonghou.common;
import java.io.Serializable;


/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016��3��2�� ����4:43:24 
 * @version v1��0
 */
public class ReduceDataFlowStatistics implements Serializable{
    private static final long serialVersionUID = -5240152692961888098L;
  //  double dsMemToDiskCombineRecsSelect;  //���ڴ������д��disk��combineǰ��kv��Ŀ��ת����
    
    public double dsShuffleMemToDiskCombineSizeSelect;
    public double dsShuffleMemToDiskCombineRecsSelect;
    
    public double dsMemToDiskCombineSizeSelect;   //combineǰ��size��ת����
    public double dsMemToDiskCombineRecsSelect;  //combineǰ��recs��ת����
    
    public double dsReduceNumSelect;   // ��������reduce����
    public double dsReduceSizeSelect;  //ִ��reduce������ ,����������������ı仯
    
}

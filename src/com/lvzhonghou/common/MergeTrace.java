package com.lvzhonghou.common;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年7月12日 下午3:58:46 
 * @version v1。0
 */
public class MergeTrace {
    public long mergeSize; //合并阶段的文件大小
    public long imediateMergeCost;  //中间合并的开销
    public long mergeCost;  //merge开销
    public long mergeReadCost;  //mersge read开销
    public long mergeWriteCost;  //merge write开销
    public boolean haveCombineWithMerge;  //merge阶段是否存在combine
    public long mergeOutRecs;  //merge输出kv数
}

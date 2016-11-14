package com.lvzhonghou.Prophet;

import java.io.Serializable;

/** 
 * @Description 
 * @author zhonghou.lzh
 * @date 2016年1月18日 下午5:34:27 
 * @version v1。0
 */
public class MapTaskParameters implements Serializable{
    /** @Fields serialVersionUID: */
    private static final long serialVersionUID = 9137058353891601844L;
    
    public double pSplitSize ;
    public double pSortMB ;
    public double pSpillPerc;
    public boolean pIsCombine;
    public double pSortFactor;
    public double pNumSpillForComb;
}

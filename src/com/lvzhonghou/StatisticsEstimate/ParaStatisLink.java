package com.lvzhonghou.StatisticsEstimate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.Prophet.ReduceTaskParameters;
import com.lvzhonghou.common.MapCostStatistics;
import com.lvzhonghou.common.MapDataFlowStatistics;
import com.lvzhonghou.common.MapOrReduce;
import com.lvzhonghou.common.ReduceCostStatistics;
import com.lvzhonghou.common.ReduceDataFlowStatistics;
import com.lvzhonghou.common.ReflectionTool;
import com.lvzhonghou.common.Statistics;
import com.lvzhonghou.common.StatisticsType;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年7月24日 下午4:18:55
 * @version v1。0
 */
public class ParaStatisLink {
    public List<Statistic> inactiveStatis; // the inactive statistics
    public List<Statistic> activeStatis; // the active statistics
    public List<Parameter> parameters; // all the parameters
    public Map<Parameter, ArrayList<Statistic>> paraStatisMap; // the map
							       // between the
							       // parameter and
							       // statistics

    public String[] commonStrs(String[] strs1, String[] strs2) {
	List<String> commons = new ArrayList<String>();
	for (String str : strs1) {
	    if (isContain(str, strs2)) {
		commons.add(str);
	    }
	}

	if (commons.size() == 0)
	    return null;

	String[] result = new String[commons.size()];
	for (int i = 0; i < result.length; i++) {
	    result[i] = commons.get(i);
	}

	return result;
    }

    public boolean isContain(String target, String[] strs) {
	if (strs.length == 0)
	    return false;
	for (String str : strs)
	    if (target.equals(str))
		return true;

	return false;
    }

    public void init() {
	// active parameters, no-tradeoff parameters and active statistics
	String[] activeParameters = new String[] { "pSortFactor", "pCopyThread" };
	String[] noTradeOffParas = new String[] { "pSortMB", "pIsCombine",
		"pTotalMemorySize", "pReduceInBufPerc", "pHaveCombiner" }; // 暂时不去调整的参数
	String[] activeStatistics = new String[] { "csMergeReadCost",
		"csMergeCombineCost", "csMergeWriteCost",
		"csShuffleDiskToDiskMerge", "csShuffleDiskToDiskWrite",
		"csImeDiskToDiskMerge", "csImeDiskToDiskWrite",
		"csShuffleInMem", "csShuffleOnDisk" };

	Map<String, ArrayList<String>> paraStatisMapStr = new HashMap<String, ArrayList<String>>();
	ArrayList<String> arr1 = new ArrayList<String>();
	arr1.add(activeStatistics[0]);
	arr1.add(activeStatistics[1]);
	arr1.add(activeStatistics[2]);
	arr1.add(activeStatistics[3]);
	arr1.add(activeStatistics[4]);
	arr1.add(activeStatistics[5]);
	arr1.add(activeStatistics[6]);
	paraStatisMapStr.put(activeParameters[0], arr1);
	ArrayList<String> arr2 = new ArrayList<String>();
	arr2.add(activeStatistics[7]);
	arr2.add(activeStatistics[8]);
	paraStatisMapStr.put(activeParameters[1], arr2);

	String[] mapParas = ReflectionTool
		.getPropertyNames(MapTaskParameters.class);
	String[] reduceParas = ReflectionTool
		.getPropertyNames(ReduceTaskParameters.class);
	String[] mapandreduceParas = commonStrs(mapParas, reduceParas);
	parameters = new ArrayList<Parameter>();
	// add the map and reduce task parameters
	if (mapandreduceParas != null) {
	    for (String para : mapandreduceParas) {
		boolean isActive = isContain(para, activeParameters);
		boolean isTradeOff = !isContain(para, noTradeOffParas);
		Parameter parameter = new Parameter(para,
			MapOrReduce.mapandreduce, isTradeOff, isActive);
		parameters.add(parameter);
	    }
	}
	// add the map task parameters
	for (String para : mapParas) {
	    if (isContain(para, mapandreduceParas))
		continue;
	    boolean isActive = isContain(para, activeParameters);
	    boolean isTradeOff = !isContain(para, noTradeOffParas);
	    Parameter parameter = new Parameter(para, MapOrReduce.map,
		    isTradeOff, isActive);
	    parameters.add(parameter);
	}
	// add the reduce task parameters
	for (String para : reduceParas) {
	    if (isContain(para, mapandreduceParas))
		continue;
	    boolean isActive = isContain(para, activeParameters);
	    boolean isTradeOff = !isContain(para, noTradeOffParas);
	    Parameter parameter = new Parameter(para, MapOrReduce.reduce,
		    isTradeOff, isActive);
	    parameters.add(parameter);
	}

	String[] mapCostStatis = ReflectionTool
		.getPropertyNames(MapCostStatistics.class);
	String[] mapDataflowStatis = ReflectionTool
		.getPropertyNames(MapDataFlowStatistics.class);
	String[] reduceCostStatis = ReflectionTool
		.getPropertyNames(ReduceCostStatistics.class);
	String[] reduceDataflowStatis = ReflectionTool
		.getPropertyNames(ReduceDataFlowStatistics.class);
	
	// init the inactiveStatis and activeStatis
	inactiveStatis = new ArrayList<Statistic>();
	activeStatis = new ArrayList<Statistic>();
	// add the map cost statistics
	for (String statis : mapCostStatis) {
	    Statistic statistic = null;
	    boolean isActive = isContain(statis, activeStatistics);
	    if (isActive) {
		statistic = new Statistic(statis, MapOrReduce.map,
			StatisticsType.cost, true);
		activeStatis.add(statistic);
	    } else {
		statistic = new Statistic(statis, MapOrReduce.map,
			StatisticsType.cost, false);
		inactiveStatis.add(statistic);
	    }
	}
	// add the map dataflow statistics
	for (String statis : mapDataflowStatis) {
	    Statistic statistic = null;
	    boolean isActive = isContain(statis, activeStatistics);
	    if (isActive) {
		statistic = new Statistic(statis, MapOrReduce.map,
			StatisticsType.dataflow, true);
		activeStatis.add(statistic);
	    } else {
		statistic = new Statistic(statis, MapOrReduce.map,
			StatisticsType.dataflow, false);
		inactiveStatis.add(statistic);
	    }
	}
	// add the reduce cost statistics
	for (String statis : reduceCostStatis) {
	    Statistic statistic = null;
	    boolean isActive = isContain(statis, activeStatistics);
	    if (isActive) {
		statistic = new Statistic(statis, MapOrReduce.reduce,
			StatisticsType.cost, true);
		activeStatis.add(statistic);
	    } else {
		statistic = new Statistic(statis, MapOrReduce.reduce,
			StatisticsType.cost, false);
		inactiveStatis.add(statistic);
	    }
	}
	// add the reduce dataflow statistics
	for (String statis : reduceDataflowStatis) {
	    Statistic statistic = null;
	    boolean isActive = isContain(statis, activeStatistics);
	    if (isActive) {
		statistic = new Statistic(statis, MapOrReduce.reduce,
			StatisticsType.dataflow, true);
		activeStatis.add(statistic);
	    } else {
		statistic = new Statistic(statis, MapOrReduce.reduce,
			StatisticsType.dataflow, false);
		inactiveStatis.add(statistic);
	    }
	}

	// init the paraStatisMap
	paraStatisMap = new HashMap<Parameter, ArrayList<Statistic>>();
	for (Map.Entry<String, ArrayList<String>> entry : paraStatisMapStr
		.entrySet()) {
	    String paraStr = entry.getKey();
	    ArrayList<String> statisStrs = entry.getValue();

	    // search the paraStr from the parameters
	    Parameter para = null;
	    for (Parameter parameter : parameters) {
		if (paraStr.equals(parameter.paraName)) {
		    para = parameter;
		    break;
		}
	    }
	    // search the statistics from the activeStatistics
	    ArrayList<Statistic> statistics = new ArrayList<Statistic>();
	    for (String statisStr : statisStrs) {
		for (Statistic statis : activeStatis) {
		    if (statisStr.equals(statis.name)) {
			statistics.add(statis);
			break;
		    }
		}
	    }

	    paraStatisMap.put(para, statistics);
	}

    }

}
package com.lvzhonghou.LTrace;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.Prophet.ReduceTaskParameters;
import com.lvzhonghou.common.DiskMergeInShuffle;
import com.lvzhonghou.common.MapOrReduce;
import com.lvzhonghou.common.MapStatistics;
import com.lvzhonghou.common.MapTraceData;
import com.lvzhonghou.common.MemMergeInShuffle;
import com.lvzhonghou.common.ReduceStatistics;
import com.lvzhonghou.common.ReduceTraceData;
import com.lvzhonghou.common.ShuffleInfo;
import com.lvzhonghou.common.ShuffleType;
import com.lvzhonghou.common.SpillTrace;
import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年1月16日 下午11:17:04
 * @version v1。0
 */
public class StatisticsExtract {
    static List<MapTraceData> mapTraces = new ArrayList<MapTraceData>();
    static List<MapStatistics> mapStatistics = new ArrayList<MapStatistics>();
    static MapStatistics mapStatistic = new MapStatistics();
    static List<ReduceTraceData> reduceTraces = new ArrayList<ReduceTraceData>();
    static List<ReduceStatistics> reduceStatistics = new ArrayList<ReduceStatistics>();
    static ReduceStatistics reduceStatistic = new ReduceStatistics();
    static MapTaskParameters mapParameters = new MapTaskParameters();
    static ReduceTaskParameters reduceParameters = new ReduceTaskParameters();

    /*
     * double csSortCost; //排序开销 double csCombineCost; //combine开销 double
     * csCombineReadCost; //读缓冲区的开销，对缓冲区的数据进行反序列化的开销 double csCombineWriteCost;
     * 
     * double csMergeReadCost; //merge读取文件、反序列化开销 double csMergeCombineCost;
     * //merge combine开销 double csMergeWriteCost; //merge序列化、写文件
     */
    public static void printStatistics() {

	System.out.println("csShuffleInMem is "
		+ reduceStatistic.reduceCostStatistics.csShuffleInMem);
	System.out.println("csShuffleOnDisk is "
		+ reduceStatistic.reduceCostStatistics.csShuffleOnDisk);

	/*
	 * System.out.println("csSortCost is " +
	 * mapStatistic.costStat.csSortCost);
	 * System.out.println("csCombineCost is " +
	 * mapStatistic.costStat.csCombineCost);
	 * System.out.println("csCombineReadCost is " +
	 * mapStatistic.costStat.csCombineReadCost);
	 * System.out.println("csCombineWriteCost is " +
	 * mapStatistic.costStat.csCombineWriteCost);
	 */

	/*
	 * System.out.println("csMergeReadCost is " +
	 * mapStatistic.costStat.csMergeReadCost);
	 * System.out.println("csMergeCombineCost is " +
	 * mapStatistic.costStat.csMergeCombineCost);
	 * System.out.println("csMergeWriteCost is " +
	 * mapStatistic.costStat.csMergeWriteCost);
	 */
    }

    public static void extractParameters() {
	// extract the mapTaskParameters
	double mSize = 1024 * 1024;   // the byte amount of 1 MB
	mapParameters.pSplitSize = mapTraces.get(0).inputFileSize * 1.0 / mSize;
	mapParameters.pSortMB = mapTraces.get(0).sortMB * 1.0;  // MB
	mapParameters.pSpillPerc = (mapTraces.get(0).sortLimit * 1.0)
		/ (mapTraces.get(0).sortMB * mSize) ;

	mapParameters.pSortFactor = mapTraces.get(0).sortFactor * 1.0;
	mapParameters.pNumSpillForComb = mapTraces.get(0).numSpillForComb * 1.0;

	// calculate the pCopyThread
	List<ShuffleInfo> shuffles = reduceTraces.get(0).shuffles;
	HashMap<String, Integer> copyThreads = new HashMap<String, Integer>();
	for (ShuffleInfo shuffle : shuffles) {
	    copyThreads.put(shuffle.fetchThread, 1);
	}
	reduceParameters.pCopyThread = copyThreads.size() * 1.0;
	reduceParameters.pMergeThresholdPercent = (reduceTraces.get(0).mergeThreshold * 1.0)
		/ (reduceTraces.get(0).memoryLimit * 1.0);
	reduceParameters.pSingleShuffleMemoryLimitPercent = reduceTraces.get(0).singleShuffleMemoryLimitPercent;
	reduceParameters.pSortFactor = reduceTraces.get(0).ioSortFactor;
    }

    public static Statistics extractStatistics(String[] filePaths,
	    int mapInputSize) {
	// extract and output the start and end of all the log files
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	List<String> mapLogs = new ArrayList<String>();
	List<String> reduceLogs = new ArrayList<String>();
	for (String logFile : filePaths) {
	    boolean isCompleteFile = LogFileClassification.isComplete(logFile);
	    if (isCompleteFile) {
		MapOrReduce mapOrReduce = LogFileClassification
			.fileClassify(logFile);
		if (mapOrReduce == MapOrReduce.map) {
		    mapLogs.add(logFile);
		} else {
		    reduceLogs.add(logFile);
		}
	    }
	}
	List<TaskSEPoint> taskSEPoints = new ArrayList<TaskSEPoint>(); // records
								       // the
								       // start
								       // and
								       // end of
								       // all
								       // the
								       // tasks
	for (String mapLog : mapLogs) {
	    TaskSEPoint task = new TaskSEPoint();
	    task.mapOrReduce = MapOrReduce.map;

	    String firstLine = LogFileClassification.getFirstLine(mapLog);
	    String lastLine = LogFileClassification.getLastLine(mapLog);
	    String[] firstLogs = firstLine.split(" ");
	    String[] lastLogs = lastLine.split(" ");

	    try {
		task.start = sdf.parse(firstLogs[0] + " " + firstLogs[1]);
		task.end = sdf.parse(lastLogs[0] + " " + lastLogs[1]);
	    } catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    taskSEPoints.add(task);
	}

	for (String reduceLog : reduceLogs) {
	    TaskSEPoint task = new TaskSEPoint();
	    task.mapOrReduce = MapOrReduce.reduce;

	    String firstLine = LogFileClassification.getFirstLine(reduceLog);
	    String lastLine = LogFileClassification.getLastLine(reduceLog);
	    String[] firstLogs = firstLine.split(" ");
	    String[] lastLogs = lastLine.split(" ");

	    try {
		task.start = sdf.parse(firstLogs[0] + " " + firstLogs[1]);
		task.end = sdf.parse(lastLogs[0] + " " + lastLogs[1]);
	    } catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    taskSEPoints.add(task);
	}

	List<TaskSEPoint> S1 = new ArrayList<TaskSEPoint>(); // start
	List<TaskSEPoint> S2 = new ArrayList<TaskSEPoint>(); // end
	for (TaskSEPoint task : taskSEPoints) {
	    TaskSEPoint newTask1 = new TaskSEPoint(task, 0);
	    S1.add(newTask1);
	    TaskSEPoint newTask2 = new TaskSEPoint(task, 1);
	    S2.add(newTask2);
	}
	Collections.sort(S1);
	Collections.sort(S2);

	List<Long> scheduleCosts = new ArrayList<Long>();
	for (TaskSEPoint task2 : S2) {
	    if (S1.size() == 0)
		break;
	    while (S1.get(0).start.getTime() <= task2.end.getTime()) {
		S1.remove(0);
		if (S1.size() == 0)
		    break;
	    }

	    if (S1.size() != 0) {
		long scheduleCost = S1.get(0).start.getTime()
			- task2.end.getTime();
		scheduleCosts.add(scheduleCost);
		S1.remove(0);
	    }
	}

	double scheduleCostAVG, scheduleCostSum = 0;
	for (long scheduleCost : scheduleCosts) {
	    scheduleCostSum += scheduleCost * 1.0;
	}
	scheduleCostAVG = scheduleCostSum / scheduleCosts.size();

	// to do
	Statistics statistics = null;

	List<String> mapFiles = new ArrayList<String>();
	List<String> reduceFiles = new ArrayList<String>();

	for (String file : filePaths) {
	    // 是否完整，若完整，则判断mapFiles还是reduceFiles
	    boolean isComplete = LogFileClassification.isComplete(file);
	    if (isComplete) {
		MapOrReduce mapOrReduce = LogFileClassification
			.fileClassify(file);
		if (mapOrReduce == MapOrReduce.map) {
		    if ((LogFileClassification.getSplitSize(file) * 1.0)
			    / (mapInputSize * 1024 * 1024 * 1.0) > 0.8)
			mapFiles.add(file);
		} else {
		    reduceFiles.add(file);
		}
	    }
	}

	int mFiles = mapFiles.size();
	int rFiles = reduceFiles.size();

	for (String str : mapFiles) {
	    System.out.println("mapfile: " + str);
	}
	for (String str : reduceFiles) {
	    System.out.println("reducefile: " + str);
	}
	/*
	 * if (mFiles > 0) { String start, end; int mapIndex = 0; // extract and
	 * output the start and end of the log file for (String file : mapFiles)
	 * { String firstLine = LogFileClassification.getFirstLine(file);
	 * String[] logArr = firstLine.split(" "); start = logArr[0] +
	 * logArr[1]; String lastLine = LogFileClassification.getLastLine(file);
	 * String[] logArr1 = lastLine.split(" "); end = logArr1[0] +
	 * logArr1[1]; System.out.println("the map " + mapIndex + " start is " +
	 * start + " and the end is " + end); mapIndex++; }
	 * 
	 * } if (rFiles > 0) { String start, end; int reduceIndex = 0; //
	 * extract and output the start and end of the log file for (String file
	 * : reduceFiles) { String firstLine =
	 * LogFileClassification.getFirstLine(file); String[] logArr =
	 * firstLine.split(" "); start = logArr[0] + logArr[1]; String lastLine
	 * = LogFileClassification.getLastLine(file); String[] logArr1 =
	 * lastLine.split(" "); end = logArr1[0] + logArr1[1];
	 * System.out.println("the reduce " + reduceIndex + " start is " + start
	 * + " and the end is " + end); reduceIndex++; } }
	 */
	if (mFiles + rFiles > 0) {
	    statistics = new Statistics();

	    if (mFiles > 0) {
		String[] maps = new String[mFiles];
		int i = 0;
		for (String ff : mapFiles)
		    maps[i++] = ff;
		extractMapStatistic(maps);
		mapStatistic.costStat.csScheduleCost = scheduleCostAVG;
		statistics.mapStatistics = mapStatistic;
	    }

	    if (rFiles > 0) {
		String[] reduces = new String[rFiles];
		int i = 0;
		for (String ff : reduceFiles)
		    reduces[i++] = ff;
		extractReduceStatistic(reduces);
		reduceStatistic.reduceCostStatistics.csSheduleCost = scheduleCostAVG;
		statistics.reduceStatistics = reduceStatistic;
	    }
	}

	// extract the parameters from the first mapTrace or 
	extractParameters();
	statistics.mapParameters = mapParameters;
	statistics.reduceParameters = reduceParameters;
	
	return statistics;
    }

    /**
     * @Description
     * @param filePaths
     */
    public static void extractReduceStatistic(String[] filePaths) {
	extractReduceTraces(filePaths);
	ReduceStatistics tempStatistic = new ReduceStatistics();

	for (ReduceStatistics statistics : reduceStatistics) {
	    tempStatistic.reduceCostStatistics.initialCost += statistics.reduceCostStatistics.initialCost;
	    tempStatistic.reduceCostStatistics.csShuffleInMem += statistics.reduceCostStatistics.csShuffleInMem;
	    tempStatistic.reduceCostStatistics.csShuffleOnDisk += statistics.reduceCostStatistics.csShuffleOnDisk;
	    tempStatistic.reduceCostStatistics.csShuffleMemToDiskMerge += statistics.reduceCostStatistics.csShuffleMemToDiskMerge;
	    tempStatistic.reduceCostStatistics.csShuffleMemToDiskCombine += statistics.reduceCostStatistics.csShuffleMemToDiskCombine;
	    tempStatistic.reduceCostStatistics.csShuffleMemToDiskWrite += statistics.reduceCostStatistics.csShuffleMemToDiskWrite;
	    tempStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge += statistics.reduceCostStatistics.csShuffleDiskToDiskMerge;
	    tempStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite += statistics.reduceCostStatistics.csShuffleDiskToDiskWrite;
	    tempStatistic.reduceCostStatistics.csImeDiskToDiskMerge += statistics.reduceCostStatistics.csImeDiskToDiskMerge;
	    tempStatistic.reduceCostStatistics.csImeDiskToDiskWrite += statistics.reduceCostStatistics.csImeDiskToDiskWrite;
	    tempStatistic.reduceCostStatistics.csImeMemToDiskMerge += statistics.reduceCostStatistics.csImeMemToDiskMerge;
	    tempStatistic.reduceCostStatistics.csImeMemToDiskCombine += statistics.reduceCostStatistics.csImeMemToDiskCombine;
	    tempStatistic.reduceCostStatistics.csImeMemToDiskWrite += statistics.reduceCostStatistics.csImeMemToDiskWrite;

	    tempStatistic.reduceCostStatistics.csFinalMemToDiskMerge += statistics.reduceCostStatistics.csFinalMemToDiskMerge;
	    tempStatistic.reduceCostStatistics.csFinalMemToDiskWrite += statistics.reduceCostStatistics.csFinalMemToDiskWrite;
	    tempStatistic.reduceCostStatistics.csFinalDiskToDiskMerge += statistics.reduceCostStatistics.csFinalDiskToDiskMerge;
	    tempStatistic.reduceCostStatistics.csReduce += statistics.reduceCostStatistics.csReduce;
	    tempStatistic.reduceCostStatistics.csHdfsWrite += statistics.reduceCostStatistics.csHdfsWrite;

	    tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect += statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect;
	    tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect += statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect;
	    tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect += statistics.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect;
	    tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect += statistics.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect;
	    tempStatistic.reduceDataFlowStatistics.dsReduceSizeSelect += statistics.reduceDataFlowStatistics.dsReduceSizeSelect;
	    tempStatistic.reduceDataFlowStatistics.dsReduceNumSelect += statistics.reduceDataFlowStatistics.dsReduceNumSelect;
	}

	int nums = reduceStatistics.size();
	reduceStatistic.reduceCostStatistics.initialCost = tempStatistic.reduceCostStatistics.initialCost
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleInMem = tempStatistic.reduceCostStatistics.csShuffleInMem
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleOnDisk = tempStatistic.reduceCostStatistics.csShuffleOnDisk
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge = tempStatistic.reduceCostStatistics.csShuffleMemToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleMemToDiskCombine = tempStatistic.reduceCostStatistics.csShuffleMemToDiskCombine
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleMemToDiskWrite = tempStatistic.reduceCostStatistics.csShuffleMemToDiskWrite
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge = tempStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite = tempStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite
		/ nums;
	reduceStatistic.reduceCostStatistics.csImeMemToDiskMerge = tempStatistic.reduceCostStatistics.csImeMemToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csImeMemToDiskCombine = tempStatistic.reduceCostStatistics.csImeMemToDiskCombine
		/ nums;
	reduceStatistic.reduceCostStatistics.csImeMemToDiskWrite = tempStatistic.reduceCostStatistics.csImeMemToDiskWrite
		/ nums;
	reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge = tempStatistic.reduceCostStatistics.csImeDiskToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite = tempStatistic.reduceCostStatistics.csImeDiskToDiskWrite
		/ nums;

	reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge = tempStatistic.reduceCostStatistics.csFinalMemToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite = tempStatistic.reduceCostStatistics.csFinalMemToDiskWrite
		/ nums;
	reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge = tempStatistic.reduceCostStatistics.csFinalDiskToDiskMerge
		/ nums;
	reduceStatistic.reduceCostStatistics.csReduce = tempStatistic.reduceCostStatistics.csReduce
		/ nums;
	reduceStatistic.reduceCostStatistics.csHdfsWrite = tempStatistic.reduceCostStatistics.csHdfsWrite
		/ nums;

	reduceStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect = tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect = tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect = tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect = tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect = tempStatistic.reduceDataFlowStatistics.dsReduceSizeSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect = tempStatistic.reduceDataFlowStatistics.dsReduceNumSelect
		/ nums;
	printStatistics();
    }

    /**
     * @Description
     * @param filePaths
     */
    public static void extractMapStatistic(String[] filePaths) {
	extractMapTraces(filePaths);
	MapStatistics tempStatistic = new MapStatistics();
	// remove the invalid statistic
	for (MapStatistics statistics : mapStatistics) {
	    tempStatistic.costStat.csInitialCost += statistics.costStat.csInitialCost;
	    tempStatistic.costStat.csMapCost += statistics.costStat.csMapCost; // csMapCostNoSpill
	    tempStatistic.costStat.csMapCostWithSpill += statistics.costStat.csMapCostWithSpill; // csMapCostWithSpill
	    tempStatistic.costStat.csReadCost += statistics.costStat.csReadCost;
	    tempStatistic.costStat.csSortCost += statistics.costStat.csSortCost;
	    tempStatistic.costStat.csCombineCost += statistics.costStat.csCombineCost;
	    tempStatistic.costStat.csCombineReadCost += statistics.costStat.csCombineReadCost;
	    tempStatistic.costStat.csCombineWriteCost += statistics.costStat.csCombineWriteCost;
	    tempStatistic.costStat.csMergeReadCost += statistics.costStat.csMergeReadCost;
	    tempStatistic.costStat.csMergeCombineCost += statistics.costStat.csMergeCombineCost;
	    tempStatistic.costStat.csMergeWriteCost += statistics.costStat.csMergeWriteCost;

	    tempStatistic.dataFlowStat.dsInputPairWidth += statistics.dataFlowStat.dsInputPairWidth;
	    tempStatistic.dataFlowStat.dsMapRecsSelect += statistics.dataFlowStat.dsMapRecsSelect;
	    tempStatistic.dataFlowStat.dsMapOutRecWidth += statistics.dataFlowStat.dsMapOutRecWidth;
	    tempStatistic.dataFlowStat.dsCombineRecsSelect += statistics.dataFlowStat.dsCombineRecsSelect;
	    tempStatistic.dataFlowStat.dsSpillOutRecWidth += statistics.dataFlowStat.dsSpillOutRecWidth;
	    tempStatistic.dataFlowStat.dsMergeCombineRecsSelect += statistics.dataFlowStat.dsMergeCombineRecsSelect;
	}

	int nums = mapStatistics.size();
	mapStatistic.costStat.csInitialCost = tempStatistic.costStat.csInitialCost
		/ nums;
	mapStatistic.costStat.csMapCost = tempStatistic.costStat.csMapCost
		/ nums;
	mapStatistic.costStat.csMapCostWithSpill = tempStatistic.costStat.csMapCostWithSpill
		/ nums;
	mapStatistic.costStat.csReadCost = tempStatistic.costStat.csReadCost
		/ nums;
	mapStatistic.costStat.csSortCost = tempStatistic.costStat.csSortCost
		/ nums;
	mapStatistic.costStat.csCombineCost = tempStatistic.costStat.csCombineCost
		/ nums;
	mapStatistic.costStat.csCombineReadCost = tempStatistic.costStat.csCombineReadCost
		/ nums;
	mapStatistic.costStat.csCombineWriteCost = tempStatistic.costStat.csCombineWriteCost
		/ nums;
	mapStatistic.costStat.csMergeReadCost = tempStatistic.costStat.csMergeReadCost
		/ nums;
	mapStatistic.costStat.csMergeCombineCost = tempStatistic.costStat.csMergeCombineCost
		/ nums;
	mapStatistic.costStat.csMergeWriteCost = tempStatistic.costStat.csMergeWriteCost
		/ nums;

	mapStatistic.dataFlowStat.dsInputPairWidth = tempStatistic.dataFlowStat.dsInputPairWidth
		/ nums;
	mapStatistic.dataFlowStat.dsMapRecsSelect = tempStatistic.dataFlowStat.dsMapRecsSelect
		/ nums;
	mapStatistic.dataFlowStat.dsMapOutRecWidth = tempStatistic.dataFlowStat.dsMapOutRecWidth
		/ nums;
	mapStatistic.dataFlowStat.dsCombineRecsSelect = tempStatistic.dataFlowStat.dsCombineRecsSelect
		/ nums;
	mapStatistic.dataFlowStat.dsSpillOutRecWidth = tempStatistic.dataFlowStat.dsSpillOutRecWidth
		/ nums;
	mapStatistic.dataFlowStat.dsMergeCombineRecsSelect = tempStatistic.dataFlowStat.dsMergeCombineRecsSelect
		/ nums;
    }

    public static void extractReduceTraces(String[] filePaths) {
	for (String filePath : filePaths) {
	    LogProfiling.reduceProfile(filePath);
	    reduceTraces.add(LogProfiling.reduceTrace);
	}

	for (ReduceTraceData reduceTrace : reduceTraces) {
	    ReduceStatistics reduceStatistic = new ReduceStatistics();

	    // start the extract the statistics from the trace
	    // costStatistics
	    reduceStatistic.reduceCostStatistics.initialCost = reduceTrace.initialCost;

	    List<Double> csShuffleInMems = new ArrayList<Double>();
	    List<Double> csShuffleOnDisks = new ArrayList<Double>();

	    for (ShuffleInfo shuffle : reduceTrace.shuffles) {
		if (shuffle.type == ShuffleType.inMemory) {
		    double csShuffleInMem = shuffle.shuffleCost * 1.0
			    / (shuffle.compressLength * 1000000.0); // ms/byte
		    // System.out.println("csShuffleInMem is " +
		    // csShuffleInMem);
		    csShuffleInMems.add(csShuffleInMem);
		} else {
		    double csShuffleOnDisk = shuffle.shuffleCost * 1.0
			    / (shuffle.compressLength * 1000000.0); // ms/byte
		    csShuffleOnDisks.add(csShuffleOnDisk);
		}
	    }
	    if (csShuffleInMems.size() != 0) {
		double csSumShuffleInMem = 0;
		for (Double shuffle : csShuffleInMems) {
		    csSumShuffleInMem += shuffle;
		}
		reduceStatistic.reduceCostStatistics.csShuffleInMem = csSumShuffleInMem
			/ csShuffleInMems.size(); // ms/byte
	    }
	    if (csShuffleOnDisks.size() != 0) {
		double csSumShuffleOnDisk = 0;
		for (Double shuffle : csShuffleOnDisks) {
		    csSumShuffleOnDisk += shuffle;
		}
		reduceStatistic.reduceCostStatistics.csShuffleOnDisk = csSumShuffleOnDisk
			/ csShuffleOnDisks.size(); // ms/byte
	    }

	    // no combiner
	    double csMemMergeReadSum = 0;
	    double csMemMergeWriteSum = 0;
	    for (MemMergeInShuffle memMerge : reduceTrace.memMergesInShu) {
		csMemMergeReadSum += (memMerge.mergeCost * 1.0)
			/ (memMerge.mergeSize * 1000000.0); // ms/byte
		csMemMergeWriteSum += (memMerge.writeCost * 1.0)
			/ (memMerge.mergeSize * 1000000.0); // ms/byte
	    }
	    if (reduceTrace.memMergesInShu.size() != 0) {
		reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge = csMemMergeReadSum
			/ reduceTrace.memMergesInShu.size();
		reduceStatistic.reduceCostStatistics.csShuffleMemToDiskWrite = csMemMergeWriteSum
			/ reduceTrace.memMergesInShu.size();
	    }

	    double csDiskMergeReadSum = 0;
	    double csDiskMergeWriteSum = 0;
	    for (DiskMergeInShuffle diskMerge : reduceTrace.diskMergesInShu) {
		csDiskMergeReadSum += (diskMerge.mergeCost * 1.0)
			/ (diskMerge.mergeSize * 1000000.0); // ms/byte
		csDiskMergeWriteSum += (diskMerge.writeCost * 1.0)
			/ (diskMerge.mergeSize * 1000000.0); // ms/byte
	    }
	    if (reduceTrace.diskMergesInShu.size() != 0) {
		reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge = csDiskMergeReadSum
			/ reduceTrace.diskMergesInShu.size();
		reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite = csDiskMergeWriteSum
			/ reduceTrace.diskMergesInShu.size();
	    }

	    if (reduceTrace.finalMerge.memToDiskMergeCost != 0
		    && reduceTrace.finalMerge.memToDiskSize != 0) {
		reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge = (reduceTrace.finalMerge.memToDiskMergeCost * 1.0)
			/ (reduceTrace.finalMerge.memToDiskSize * 1000000.0); // ms/byte
		reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite = (reduceTrace.finalMerge.memToDiskWriteCost * 1.0)
			/ (reduceTrace.finalMerge.memToDiskSize * 1000000.0); // ms/byte
	    }

	    if (reduceTrace.finalMerge.immeNotFirstImmeMergeCost != 0
		    && reduceTrace.finalMerge.immeNotFirstImmeSize != 0) {
		reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge = (reduceTrace.finalMerge.immeNotFirstImmeMergeCost * 1.0)
			/ (reduceTrace.finalMerge.immeNotFirstImmeSize * 1000000.0);
		reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite = (reduceTrace.finalMerge.immeNotFirstImmeWriteCost * 1.0)
			/ (reduceTrace.finalMerge.immeNotFirstImmeSize * 1000000.0);

		// calcu the csFinalMemToDiskMerge and csFinalMemToDiskWrite
		if (reduceTrace.finalMerge.memToDiskSize != 0
			&& reduceTrace.finalMerge.immeFirstImmeMergeCost != 0) {
		    long diskSize = reduceTrace.finalMerge.immeFirstImmeSize
			    - reduceTrace.finalMerge.memToDiskSize;
		    reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge = ((reduceTrace.finalMerge.immeFirstImmeMergeCost - diskSize
			    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge) * 1.0)
			    / (reduceTrace.finalMerge.memToDiskSize * 1000000.0);
		    reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite = ((reduceTrace.finalMerge.immeFirstImmeWriteCost - diskSize
			    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite) * 1.0)
			    / (reduceTrace.finalMerge.memToDiskSize * 1000000.0);
		}
	    }

	    if (reduceTrace.finalMerge.immeFirstImmeMergeCost != 0
		    && reduceTrace.finalMerge.immeFirstImmeSize != 0) {
		if (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge == 0) {
		    reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge = (reduceTrace.finalMerge.immeFirstImmeMergeCost * 1.0)
			    / (reduceTrace.finalMerge.immeFirstImmeSize * 1000000.0);
		    reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite = (reduceTrace.finalMerge.immeFirstImmeWriteCost * 1.0)
			    / (reduceTrace.finalMerge.immeFirstImmeSize * 1000000.0);
		    reduceStatistic.reduceCostStatistics.csImeMemToDiskMerge = reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge;
		    reduceStatistic.reduceCostStatistics.csImeMemToDiskWrite = reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite;
		} else {

		}
	    }

	    // when the final merge
	    // when the final merge, the memSize = 0
	    if (reduceTrace.finalMerge.memToDiskMergeCost != 0
		    || reduceTrace.finalMerge.immeFirstImmeMergeCost != 0
		    || reduceTrace.finalMerge.memToDiskSize == 0) {
		if (reduceTrace.finalMerge.finalMergeCost != 0
			&& reduceTrace.finalMerge.finalMergeSize != 0) {
		    reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge = (reduceTrace.finalMerge.finalMergeCost * 1.0)
			    / (reduceTrace.finalMerge.finalMergeSize * 1000000.0);

		    // calcu the csFinalMemToDiskMerge and csFinalMemToDiskWrite
		    if (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge == 0
			    && reduceTrace.finalMerge.memToDiskSize != 0
			    && reduceTrace.finalMerge.immeFirstImmeMergeCost != 0) {
			long diskSize = reduceTrace.finalMerge.immeFirstImmeSize
				- reduceTrace.finalMerge.memToDiskSize;
			reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge = reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge;
			reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite = (reduceTrace.finalMerge.immeFirstImmeWriteCost * 1.0)
				/ (reduceTrace.finalMerge.immeFirstImmeSize * 1000000.0);
		    }
		}
	    }

	    reduceStatistic.reduceCostStatistics.csReduce = reduceTrace.reduceCost
		    * 1.0 / (1000000.0 * reduceTrace.numsReduce); // ms/num
	    reduceStatistic.reduceCostStatistics.csHdfsWrite = reduceTrace.hdfsWriteCost
		    * 1.0 / (1000000.0 * reduceTrace.reduceOutSize); // ms/byte

	    // dataflowStatistics
	    reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect = (reduceTrace.reduceOutSize * 1.0)
		    / (reduceTrace.finalMerge.finalMergeSize * 1.0); // %

	    reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect = (reduceTrace.numsReduce * 1.0)
		    / (reduceTrace.finalMerge.finalMergeSize * 1.0);

	    System.out
		    .println("numsReduce is "
			    + reduceTrace.numsReduce
			    + " dsReduceNumSelect is "
			    + reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect);

	    // add the reduceStatistic
	    reduceStatistics.add(reduceStatistic);
	}

    }

    /**
     * @Description
     * @param filePaths
     */
    public static void extractMapTraces(String[] filePaths) {
	for (String filePath : filePaths) {
	    System.out.println(filePath);
	    LogProfiling.mapProfile(filePath);
	    mapTraces.add(LogProfiling.trace);
	}

	for (MapTraceData mapTrace : mapTraces) {
	    MapStatistics mapStatistic = new MapStatistics();

	    // start to extract the statistics from the current mapTrace;
	    // extract the costStatistics
	    mapStatistic.costStat.csInitialCost = mapTrace.initialCost; // ms
	    mapStatistic.costStat.csReadCost = (mapTrace.readAndMapCost - mapTrace.mapCost)
		    * 1.0 / (1000000.0 * mapTrace.inputFileSize); // ms/byte
	    long mapSleep = 0;
	    for (SpillTrace spillTrace : mapTrace.spillList) {
		mapSleep += spillTrace.mainThreadSleep; // ms
	    }
	    long mapRealCost = mapTrace.mapCost / 1000000 - mapSleep; // ms
	    // mapStatistic.costStat.csMapCost = mapRealCost * 1.0 /
	    // mapTrace.maps; // ms/map
	    /*
	     * System.out.println("mapCost is " + mapTrace.mapCost / 1000000 +
	     * " mapSleep is " + mapSleep + " mapRealCost is " + mapRealCost);
	     */
	    long mapCostNoSpill = mapRealCost - mapTrace.mapCostWithSpill; // ms
	    double numMapsNoSpill = mapTrace.maps
		    * ((mapTrace.mapOutRecs - mapTrace.mapOutRecsWithSpill) * 1.0 / mapTrace.mapOutRecs); // num
	    if (numMapsNoSpill != 0)
		mapStatistic.costStat.csMapCost = mapCostNoSpill * 1.0
			/ numMapsNoSpill; // ms / maps
	    double numMapsWithSpill = mapTrace.maps - numMapsNoSpill; // num
	    if (numMapsWithSpill != 0)
		mapStatistic.costStat.csMapCostWithSpill = mapTrace.mapCostWithSpill
			* 1.0 / numMapsWithSpill; // ms / maps
	    /*
	     * System.out.println("csMapCostWithSpill is " +
	     * mapStatistic.costStat.csMapCostWithSpill + " csMapCost is " +
	     * mapStatistic.costStat.csMapCost);
	     * System.out.println("numMapsWithSpill is " + numMapsWithSpill);
	     */
	    boolean haveCombinerWithSpill = mapTrace.spillList.get(0).haveCombiner;

	    double csSortCostSum = 0;
	    for (int i = 0; i < mapTrace.spillList.size(); i++) {
		csSortCostSum += mapTrace.spillList.get(i).sortTime
			/ (1000000.0 * mapTrace.spillList.get(i).kvNums);
	    }
	    mapStatistic.costStat.csSortCost = csSortCostSum
		    / mapTrace.spillList.size(); // ms/kv
	    if (haveCombinerWithSpill) {
		double csCombineReadCostSum = 0;
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    csCombineReadCostSum += mapTrace.spillList.get(i).readTime
			    / (1000000.0 * mapTrace.spillList.get(i).kvNums);
		}
		mapStatistic.costStat.csCombineReadCost = csCombineReadCostSum
			/ mapTrace.spillList.size(); // ms/kv
	    } else {
		double csReadCostSum = 0;
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    csReadCostSum += (mapTrace.spillList.get(i).spillTime
			    - mapTrace.spillList.get(i).sortTime - mapTrace.spillList
				.get(i).writeTime)
			    / (1000000.0 * mapTrace.spillList.get(i).kvNums);
		}
		mapStatistic.costStat.csCombineReadCost = csReadCostSum
			/ mapTrace.spillList.size(); // ms/kv
	    }

	    double csCombineWriteCostSum;
	    if (haveCombinerWithSpill) {
		csCombineWriteCostSum = 0;
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    csCombineWriteCostSum += mapTrace.spillList.get(i).writeTime
			    / (1000000.0 * mapTrace.spillList.get(i).combineOutRecs); //
		}
		mapStatistic.costStat.csCombineWriteCost = csCombineWriteCostSum
			/ mapTrace.spillList.size(); // ms/kv
	    } else {
		csCombineWriteCostSum = 0;
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    csCombineWriteCostSum += mapTrace.spillList.get(i).writeTime
			    / (1000000.0 * mapTrace.spillList.get(i).kvNums);
		}
		mapStatistic.costStat.csCombineWriteCost = csCombineWriteCostSum
			/ mapTrace.spillList.size(); // ms/kv
	    }
	    double csCombineCostSum = 0;
	    if (haveCombinerWithSpill) {
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    long combineTime = mapTrace.spillList.get(i).combineAndWrite
			    - mapTrace.spillList.get(i).readTime
			    - mapTrace.spillList.get(i).writeTime;
		    csCombineCostSum += combineTime
			    / (1000000.0 * mapTrace.spillList.get(i).kvNums);
		}
		mapStatistic.costStat.csCombineCost = csCombineCostSum
			/ mapTrace.spillList.size(); // ms/kv
	    } else {
		mapStatistic.costStat.csCombineCost = 0;
	    }

	    // calculate
	    int numMergeKV = 0;
	    if (haveCombinerWithSpill) {
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    numMergeKV += mapTrace.spillList.get(i).combineOutRecs;
		}
	    } else {
		for (int i = 0; i < mapTrace.spillList.size(); i++) {
		    numMergeKV += mapTrace.spillList.get(i).kvNums;
		}
	    }

	    int numMergeKVPerMerge = numMergeKV / (mapTrace.mergeList.size());
	    boolean haveCombinerWithMerge = mapTrace.mergeList.get(0).haveCombineWithMerge;
	    double csMergeReadCostSum = 0, csMergeWriteCostSum = 0, csMergeCombineCostSum = 0;
	    for (int i = 0; i < mapTrace.mergeList.size(); i++) {
		if (haveCombinerWithMerge) {
		    csMergeReadCostSum += mapTrace.mergeList.get(i).mergeReadCost
			    / (1000000.0 * numMergeKVPerMerge); // ms/kv
		    csMergeWriteCostSum += mapTrace.mergeList.get(i).mergeWriteCost
			    / (1000000.0 * mapTrace.mergeList.get(i).mergeOutRecs); // ms/kv
		    csMergeCombineCostSum += (mapTrace.mergeList.get(i).mergeCost
			    - mapTrace.mergeList.get(i).mergeReadCost - mapTrace.mergeList
				.get(i).mergeWriteCost)
			    / (1000000.0 * numMergeKVPerMerge); // ms/kv
		} else {
		    csMergeReadCostSum += mapTrace.mergeList.get(i).mergeReadCost
			    / (1000000.0 * numMergeKVPerMerge); // ms/kv
		    csMergeWriteCostSum += mapTrace.mergeList.get(i).mergeWriteCost
			    / (1000000.0 * numMergeKVPerMerge); // ms/kv
		    csMergeCombineCostSum += 0;
		}
	    }
	    mapStatistic.costStat.csMergeReadCost = csMergeReadCostSum
		    / mapTrace.mergeList.size();
	    mapStatistic.costStat.csMergeWriteCost = csMergeWriteCostSum
		    / mapTrace.mergeList.size();
	    mapStatistic.costStat.csMergeCombineCost = csMergeCombineCostSum
		    / mapTrace.mergeList.size();
	    // extract the dataFlowStatistics

	    mapStatistic.dataFlowStat.dsInputPairWidth = mapTrace.inputFileSize
		    / mapTrace.maps; // byte/行

	    long kvNumsSum = 0;
	    for (SpillTrace currentTrace : mapTrace.spillList) {
		kvNumsSum += currentTrace.kvNums;
	    }
	    mapStatistic.dataFlowStat.dsMapRecsSelect = kvNumsSum
		    / mapTrace.maps; // kvs/map
	    long kvNumsSumBesideLast = 0;
	    for (int i = 0; i < mapTrace.spillList.size() - 1; i++) {
		kvNumsSumBesideLast += mapTrace.spillList.get(i).kvNums;
	    }
	    long kvNumsPerSpill = 0;
	    if (mapTrace.spillList.size() - 1 != 0)
		kvNumsPerSpill = kvNumsSumBesideLast
			/ (mapTrace.spillList.size() - 1);
	    else
		kvNumsPerSpill = 0;
	    if (kvNumsPerSpill != 0)
		mapStatistic.dataFlowStat.dsMapOutRecWidth = mapTrace.sortLimit
			/ kvNumsPerSpill; // byte/kv
	    else
		mapStatistic.dataFlowStat.dsMapOutRecWidth = 0;

	    if (haveCombinerWithSpill) {
		double combineRecsSelectSum = 0;
		for (SpillTrace currentTrace : mapTrace.spillList) {
		    combineRecsSelectSum += currentTrace.combineOutRecs * 1.0
			    / currentTrace.kvNums;
		}
		mapStatistic.dataFlowStat.dsCombineRecsSelect = combineRecsSelectSum
			/ mapTrace.spillList.size();
	    } else {
		mapStatistic.dataFlowStat.dsCombineRecsSelect = 1;
	    }

	    // the sum of spill files's size
	    int sumSpillSize = 0;
	    for (SpillTrace trace : mapTrace.spillList) {
		sumSpillSize += trace.spillOutFile;
	    }

	    mapStatistic.dataFlowStat.dsSpillOutRecWidth = sumSpillSize * 1.0
		    / numMergeKV;
	    int mergeOutRecsSum = 0;
	    for (int i = 0; i < mapTrace.mergeList.size(); i++) {
		mergeOutRecsSum += mapTrace.mergeList.get(i).mergeOutRecs;
	    }
	    if (haveCombinerWithMerge) {
		mapStatistic.dataFlowStat.dsMergeCombineRecsSelect = (mergeOutRecsSum * 1.0)
			/ (numMergeKV * 1.0);
	    } else {
		mapStatistic.dataFlowStat.dsMergeCombineRecsSelect = 1;

	    }

	    // add the mapStatistic
	    mapStatistics.add(mapStatistic);
	}
    }
}

package com.lvzhonghou.Prophet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lvzhonghou.LTrace.*;
import com.lvzhonghou.common.MapStatistics;
import com.lvzhonghou.common.ReduceStatistics;
import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年1月18日 下午5:27:33
 * @version v1。0
 */
class MapOutput {
    int outSize;
    int outRecs;
}

public class PerformanceModel {
    private static int getIndexMin(double[] arr) {
	int i = 0;
	double val = arr[i];
	for (int j = 0; j < arr.length; j++) {
	    if (arr[j] < val) {
		val = arr[j];
		i = j;
	    }
	}
	return i;
    }

    private static double getMaxVal(double[] arr) {
	double val = arr[0];

	for (int i = 0; i < arr.length; i++) {
	    if (arr[i] > val)
		val = arr[i];
	}
	return val;
    }

    private static boolean isNull(List<MapOut> mapOutputs) {
	for (MapOut mapOutput : mapOutputs) {
	    if (mapOutput.nums != 0)
		return false;
	}
	return true;
    }

    private static double getLastedClock(List<MapOut> mapOutputs) {
	for (MapOut mapoutput : mapOutputs) {
	    if (mapoutput.nums != 0)
		return mapoutput.startTime;
	}
	return -1;
    }

    private static double getMemSize(List<MapOut> mapOutputs) {
	double memSizeSum = 0;

	for (MapOut mapOutput : mapOutputs) {
	    memSizeSum += mapOutput.mapOutSize;
	}

	return memSizeSum;
    }

    private static void removeMemMergeSegments(List<MapOut> mapOutputs,
	    int numMemToDisk) {
	for (int i = 0; i < numMemToDisk; i++) {
	    mapOutputs.remove(0);
	}
    }

    // getReduceInSize(memFinalMapOuts, diskFinalMapOuts)
    private static double getReduceInSize(List<MapOut> memMapOutputs,
	    List<MapOut> diskMapOutputs) {
	double sizeSum = 0;
	for (MapOut mapoutput : memMapOutputs)
	    sizeSum += mapoutput.mapOutSize;
	for (MapOut mapoutput : diskMapOutputs)
	    sizeSum += mapoutput.mapOutSize;

	return sizeSum;
    }

    private static double calFinalMergeCost(List<MapOut> memMapOutputs,
	    List<MapOut> diskMapOutputs, ReduceStatistics reduceStatistic) {
	double memSizeSum = 0, diskSizeSum = 0;
	for (MapOut mapOutput : memMapOutputs) {
	    memSizeSum += mapOutput.mapOutSize;
	}
	for (MapOut mapOutput : diskMapOutputs) {
	    diskSizeSum += mapOutput.mapOutSize;
	}

	double mergeCost = 0;
	// cal the cost of memory merge
	if (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge != 0) {
	    mergeCost += memSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge;
	} else if (reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge != 0) {
	    mergeCost += memSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge;
	} else if (reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge != 0) {
	    mergeCost += memSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge;
	} else {
	    System.out.println("there is no csFinalMemToDiskMerge.");
	}

	// cal the cost of disk merge
	if (reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge != 0) {
	    mergeCost += diskSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge;
	} else if (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge != 0) {
	    mergeCost += diskSizeSum
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge;
	} else if (reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge != 0) {
	    mergeCost += diskSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge;
	} else if (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge != 0) {
	    mergeCost += diskSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge;
	} else {
	    System.out.println("there is no csFinalDiskToDiskMerge.");
	}

	return mergeCost;
    }

    private static double calMergeCost(List<MapOut> diskFinalMapOuts,
	    int numMapOutsToMerge, List<MapOut> memFinalMapOuts,
	    ReduceStatistics reduceStatistic) {
	// sort the mapoutputs
	Collections.sort(diskFinalMapOuts, new Comparator<MapOut>() {
	    @Override
	    public int compare(MapOut o1, MapOut o2) {
		// TODO Auto-generated method stub
		if (o1.mapOutSize < o2.mapOutSize)
		    return -1;
		else
		    return 1;
	    }
	});

	double memMapOutsSize = 0;
	double memMapOutsRecs = 0;
	double diskMapOutsSize = 0;
	double diskMapOutsRecs = 0;

	// cal the mem size
	if (memFinalMapOuts.size() != 0) {
	    for (MapOut mapOutput : memFinalMapOuts) {
		memMapOutsSize += mapOutput.mapOutSize;
		memMapOutsRecs += mapOutput.mapOutRecs;
	    }

	    memFinalMapOuts.clear();
	}

	// cal the disk Size
	for (int i = 0; i < numMapOutsToMerge; i++) {
	    MapOut mapOutput = diskFinalMapOuts.get(0);
	    diskMapOutsSize += mapOutput.mapOutSize;
	    diskMapOutsRecs += mapOutput.mapOutRecs;
	    diskFinalMapOuts.remove(0);
	}

	double mergeCost = 0;
	// cal the cost of memory merge
	if (reduceStatistic.reduceCostStatistics.csImeMemToDiskMerge != 0
		&& reduceStatistic.reduceCostStatistics.csImeMemToDiskWrite != 0) {
	    mergeCost = memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csImeMemToDiskMerge
		    + memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csImeMemToDiskWrite;

	} else if (reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge != 0) {
	    mergeCost = memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge
		    + memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csShuffleMemToDiskWrite;
	} else if (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge != 0) {
	    mergeCost = memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge
		    + memMapOutsSize
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite;
	} else {
	    System.out.println("there is no csFinalMemToDiskMerge.");
	}
	System.out.println("merge the mem cost is " + mergeCost);
	System.out.println("memMapOutsSize is " + memMapOutsSize);
	// cal the cost of disk merge
	if (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge != 0) {
	    mergeCost += diskMapOutsSize
		    * (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge + reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite);
	} else if (reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge != 0) {
	    mergeCost += diskMapOutsSize
		    * (reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge + reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite);
	} else if (reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge != 0) {
	    mergeCost += diskMapOutsSize
		    * (reduceStatistic.reduceCostStatistics.csFinalDiskToDiskMerge + reduceStatistic.reduceCostStatistics.csHdfsWrite);
	} else if (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge != 0) {
	    mergeCost += diskMapOutsSize
		    * (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge + reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite);
	} else {
	    System.out.println("there is no csImeDiskToDiskMerge.");
	}

	MapOut newMapOutput = new MapOut(memMapOutsSize + diskMapOutsSize,
		memMapOutsRecs + diskMapOutsRecs);
	diskFinalMapOuts.add(newMapOutput);
	System.out.println("the merge cost is " + mergeCost);
	return mergeCost;
    }

    private static double calCleanMemToDiskCost(List<MapOut> mapOutputs,
	    MapOut newMapOutput, ReduceStatistics reduceStatistic) {
	double mergeCost = 0;
	// calcu the total size of memMapOutputs
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	for (MapOut mapoutput : mapOutputs) {
	    mergeSizeSum += mapoutput.mapOutSize;
	    mergeRecsSum += mapoutput.mapOutRecs;
	}

	// csFinalMemToDiskMerge的准确性
	if (reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge != 0) {
	    mergeCost = mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalMemToDiskMerge
		    + mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csFinalMemToDiskWrite;
	} else if (reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge != 0) {
	    mergeCost = mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleMemToDiskMerge
		    + mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleMemToDiskWrite;
	} else if (reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge != 0) {
	    mergeCost = mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge
		    + mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite;
	} else {
	    System.out.println("there is no csFinalMemToDiskMerge.");
	}

	newMapOutput.mapOutSize = mergeSizeSum;
	newMapOutput.mapOutRecs = mergeRecsSum;

	// clear the mapOutputs
	mapOutputs.clear();
	return mergeCost;
    }

    private static double calcuMemoryMergeCostWF(List<MapOut> mapOutputs,
	    int numMemToDisk, ReduceTaskParameters parameters,
	    ReduceStatistics statistics, List<MapOut> diskMapOutputs) {
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	double memMergeCost = 0;

	for (int i = 0; i < numMemToDisk; i++) {
	    mergeSizeSum += mapOutputs.get(i).mapOutSize;
	    mergeRecsSum += mapOutputs.get(i).mapOutRecs;
	}

	MapOut diskMapout = new MapOut();
	if (!parameters.pHaveCombiner) {
	    diskMapout.mapOutSize = mergeSizeSum;
	    diskMapout.mapOutRecs = mergeRecsSum;
	    memMergeCost = mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskMerge
		    + mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskWrite;

	    /*
	     * System.out.println("mergeSizeSum is " + mergeSizeSum +
	     * " csShuffleMemToDiskMerge is " +
	     * statistics.reduceCostStatistics.csShuffleMemToDiskMerge +
	     * " csShuffleMemToDiskWrite is " +
	     * statistics.reduceCostStatistics.csShuffleMemToDiskWrite +
	     * " memMergeCost is " + memMergeCost);
	     */
	} else {
	    diskMapout.mapOutSize = mergeSizeSum
		    * statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect;
	    diskMapout.mapOutRecs = mergeRecsSum
		    * statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect;
	    memMergeCost = mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskMerge
		    + mergeRecsSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskCombine
		    + diskMapout.mapOutSize
		    * statistics.reduceCostStatistics.csShuffleMemToDiskWrite;
	}

	diskMapOutputs.add(diskMapout);

	return memMergeCost;
    }

    private static double calcuMemoryMergeCost(List<MapOut> mapOutputs,
	    int numMemToDisk, ReduceTaskParameters parameters,
	    ReduceStatistics statistics, List<MapOut> diskMapOutputs,
	    double currentClock) {
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	double memMergeCost = 0;

	for (int i = 0; i < numMemToDisk; i++) {
	    mergeSizeSum += mapOutputs.get(i).mapOutSize;
	    mergeRecsSum += mapOutputs.get(i).mapOutRecs;
	}
	/*
	 * System.out.println("numMemToDisk is " + numMemToDisk +
	 * " mergeSizeSum is " + mergeSizeSum);
	 */
	MapOut diskMapout = new MapOut();
	if (!parameters.pHaveCombiner) {
	    diskMapout.mapOutSize = mergeSizeSum;
	    diskMapout.mapOutRecs = mergeRecsSum;
	    memMergeCost = mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskMerge
		    + mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskWrite;

	    /*
	     * System.out.println("mergeSizeSum is " + mergeSizeSum +
	     * " csShuffleMemToDiskMerge is " +
	     * statistics.reduceCostStatistics.csShuffleMemToDiskMerge +
	     * " csShuffleMemToDiskWrite is " +
	     * statistics.reduceCostStatistics.csShuffleMemToDiskWrite +
	     * " memMergeCost is " + memMergeCost);
	     */
	} else {
	    diskMapout.mapOutSize = mergeSizeSum
		    * statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect;
	    diskMapout.mapOutRecs = mergeRecsSum
		    * statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect;
	    memMergeCost = mergeSizeSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskMerge
		    + mergeRecsSum
		    * statistics.reduceCostStatistics.csShuffleMemToDiskCombine
		    + diskMapout.mapOutSize
		    * statistics.reduceCostStatistics.csShuffleMemToDiskWrite;
	}
	diskMapout.clock = currentClock + memMergeCost;
	diskMapOutputs.add(diskMapout);

	return memMergeCost;

    }

    private static double calcuDiskMergeCostWF(List<MapOut> diskMapOutputs,
	    ReduceTaskParameters parameters, ReduceStatistics statistic) {
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	double mergeCost = 0;

	Collections.sort(diskMapOutputs, new Comparator<MapOut>() {
	    @Override
	    public int compare(MapOut o1, MapOut o2) {
		// TODO Auto-generated method stub
		if (o1.mapOutSize < o2.mapOutSize)
		    return -1;
		else
		    return 1;
	    }
	});

	for (int i = 0; i < parameters.pSortFactor; i++) {
	    mergeSizeSum += diskMapOutputs.get(0).mapOutSize;
	    mergeRecsSum += diskMapOutputs.get(0).mapOutRecs;
	    diskMapOutputs.remove(0);
	}
	if (statistic.reduceCostStatistics.csShuffleDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csShuffleDiskToDiskWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csShuffleDiskToDiskMerge
		    + mergeSizeSum
		    * statistic.reduceCostStatistics.csShuffleDiskToDiskWrite;
	} else if (statistic.reduceCostStatistics.csImeDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csImeDiskToDiskWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csImeDiskToDiskMerge
		    + mergeSizeSum
		    * statistic.reduceCostStatistics.csImeDiskToDiskWrite;
	} else if (statistic.reduceCostStatistics.csFinalDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csHdfsWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csFinalDiskToDiskMerge
		    + mergeSizeSum * statistic.reduceCostStatistics.csHdfsWrite;
	} else {
	    System.out.println("there is not csShuffleDiskToDiskMerge");
	}
	/*
	 * System.out.println("mergeCost is " + mergeCost +
	 * "csShuffleDiskToDiskMerge is " +
	 * statistic.reduceCostStatistics.csShuffleDiskToDiskMerge);
	 */
	MapOut newDiskMapOut = new MapOut();
	newDiskMapOut.mapOutSize = mergeSizeSum;
	newDiskMapOut.mapOutRecs = mergeRecsSum;
	diskMapOutputs.add(newDiskMapOut);

	return mergeCost;

    }

    private static double calcuDiskMergeCost(List<MapOut> diskMapOutputs,
	    ReduceTaskParameters parameters, ReduceStatistics statistic,
	    double currentClock) {
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	double mergeCost = 0;

	Collections.sort(diskMapOutputs, new Comparator<MapOut>() {
	    @Override
	    public int compare(MapOut o1, MapOut o2) {
		// TODO Auto-generated method stub
		if (o1.clock < o2.clock)
		    return -1;
		else
		    return 1;
	    }
	});

	for (int i = 0; i < parameters.pSortFactor; i++) {
	    mergeSizeSum += diskMapOutputs.get(0).mapOutSize;
	    mergeRecsSum += diskMapOutputs.get(0).mapOutRecs;
	    diskMapOutputs.remove(0);
	}
	if (statistic.reduceCostStatistics.csShuffleDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csShuffleDiskToDiskWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csShuffleDiskToDiskMerge
		    + mergeSizeSum
		    * statistic.reduceCostStatistics.csShuffleDiskToDiskWrite;
	} else if (statistic.reduceCostStatistics.csImeDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csImeDiskToDiskWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csImeDiskToDiskMerge
		    + mergeSizeSum
		    * statistic.reduceCostStatistics.csImeDiskToDiskWrite;
	} else if (statistic.reduceCostStatistics.csFinalDiskToDiskMerge != 0
		&& statistic.reduceCostStatistics.csHdfsWrite != 0) {
	    mergeCost = mergeSizeSum
		    * statistic.reduceCostStatistics.csFinalDiskToDiskMerge
		    + mergeSizeSum * statistic.reduceCostStatistics.csHdfsWrite;
	} else {
	    System.out.println("there is not csShuffleDiskToDiskMerge");
	}
	/*
	 * System.out.println("mergeCost is " + mergeCost +
	 * "csShuffleDiskToDiskMerge is " +
	 * statistic.reduceCostStatistics.csShuffleDiskToDiskMerge);
	 */
	MapOut newDiskMapOut = new MapOut();
	newDiskMapOut.mapOutSize = mergeSizeSum;
	newDiskMapOut.mapOutRecs = mergeRecsSum;
	newDiskMapOut.clock = currentClock + mergeCost;
	diskMapOutputs.add(newDiskMapOut);

	return mergeCost;

    }

    private static double calcuDiskSegmentsMergeCost(
	    List<ClockSegment> diskClockSegments,
	    ReduceTaskParameters parameters, ReduceStatistics reduceStatistic,
	    double diskThreadClock, List<ClockSegment> mergeClockSegments) {
	double mergeCost = 0;
	double mergeSizeSum = 0;
	double mergeRecsSum = 0;
	int numDiskMerge = (int) parameters.pSortFactor;

	for (int i = 0; i < numDiskMerge; i++) {
	    mergeSizeSum += diskClockSegments.get(0).size;
	    mergeRecsSum += diskClockSegments.get(0).recs;
	    diskClockSegments.remove(0);
	}
	if (reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge != 0) {
	    mergeCost = mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskMerge
		    + mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csShuffleDiskToDiskWrite;

	} else {
	    mergeCost = mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskMerge
		    + mergeSizeSum
		    * reduceStatistic.reduceCostStatistics.csImeDiskToDiskWrite;
	}

	double newDiskThreadClock = mergeCost + diskThreadClock;
	ClockSegment newClockSegment = new ClockSegment(mergeRecsSum,
		mergeSizeSum, newDiskThreadClock);
	mergeClockSegments.add(newClockSegment);

	return mergeCost;
    }

    public static double whatIfReduceTaskModel(
	    List<ReduceStatistics> reduceStatistics,
	    ReduceTaskParameters parameters, MapOut mapOutput, int numMapTasks,
	    int numReduceTasks) {
	double reduceTaskCost = 0;
	ReduceStatistics reduceStatistic = calcuAvgReduceStatistic(reduceStatistics);

	// mapout for each reduceTask
	mapOutput.mapOutRecs = mapOutput.mapOutRecs / numReduceTasks;
	mapOutput.mapOutSize = mapOutput.mapOutSize / numReduceTasks;

	//
	List<MapOut> memMapOutputs = new ArrayList<MapOut>();
	List<MapOut> diskMapOutputs = new ArrayList<MapOut>();

	double cShuffleTime = 0;
	boolean isShuffleInMem = mapOutput.mapOutSize < parameters.pTotalMemorySize
		* parameters.pSingleShuffleMemoryLimitPercent;
	if (isShuffleInMem) {
	    int usedMemory = 0;
	    while (numMapTasks > 0) {
		// add the mapoutput into the memory
		MapOut newMapOut = new MapOut();
		newMapOut.mapOutSize = mapOutput.mapOutSize;
		newMapOut.mapOutRecs = mapOutput.mapOutRecs;
		memMapOutputs.add(newMapOut);
		usedMemory += mapOutput.mapOutSize;

		// add the cost
		if (reduceStatistic.reduceCostStatistics.csShuffleInMem != 0)
		    cShuffleTime += newMapOut.mapOutSize
			    * reduceStatistic.reduceCostStatistics.csShuffleInMem;
		else
		    System.out.println("there is not csShuffleInMem.");

		// whether to merge the memMapOuts
		if (usedMemory >= parameters.pTotalMemorySize
			* parameters.pMergeThresholdPercent) {
		    // merge the memMapOutputs into the disk
		    cShuffleTime += calcuMemoryMergeCostWF(memMapOutputs,
			    memMapOutputs.size(), parameters, reduceStatistic,
			    diskMapOutputs);
		    memMapOutputs.clear();
		    usedMemory = 0;
		}

		// whether to merge the diskMapOuts
		if (diskMapOutputs.size() >= 2 * parameters.pSortFactor - 1) {
		    cShuffleTime += calcuDiskMergeCostWF(diskMapOutputs,
			    parameters, reduceStatistic);
		}

		numMapTasks--;
	    }
	} else {
	    while (numMapTasks > 0) {
		// add the mapoutput into the disk
		MapOut newMapOut = new MapOut();
		newMapOut.mapOutSize = mapOutput.mapOutSize;
		newMapOut.mapOutRecs = mapOutput.mapOutRecs;
		diskMapOutputs.add(newMapOut);

		// add the cost
		if (reduceStatistic.reduceCostStatistics.csShuffleOnDisk != 0)
		    cShuffleTime += newMapOut.mapOutSize
			    * reduceStatistic.reduceCostStatistics.csShuffleOnDisk;
		else
		    System.out.println("there is not csShuffleOnDisk");

		// whether to merge the diskMapOuts
		if (diskMapOutputs.size() >= 2 * parameters.pSortFactor - 1) {
		    cShuffleTime += calcuDiskMergeCostWF(diskMapOutputs,
			    parameters, reduceStatistic);
		}

		numMapTasks--;
	    }
	}

	// final merge
	double cFinalCleanMemToDisk = 0;
	double cFinalImmediateMerge = 0;
	if (diskMapOutputs.size() < parameters.pSortFactor) {
	    if (memMapOutputs.size() > 0) {
		// clean the memMapoutputs into the disk
		System.out.println("clean the memMapoutputs into the disk");
		MapOut newDiskFinalMapOutput = new MapOut();
		cFinalCleanMemToDisk = calCleanMemToDiskCost(memMapOutputs,
			newDiskFinalMapOutput, reduceStatistic);
		diskMapOutputs.add(newDiskFinalMapOutput);
	    }
	} else if (diskMapOutputs.size() == parameters.pSortFactor) {
	    // no immediate merge
	    System.out.println("there is no immediate merge");
	} else {
	    // have immediate merge
	    System.out.println("now, it is immediate merging!");
	    // first round
	    int numDiskMapOuts = diskMapOutputs.size();
	    int numMapOutsToMergeFirst = (numDiskMapOuts - 1)
		    % ((int) parameters.pSortFactor - 1) + 1;
	    cFinalImmediateMerge += calMergeCost(diskMapOutputs,
		    numMapOutsToMergeFirst, memMapOutputs, reduceStatistic);

	    // other rounds
	    while (diskMapOutputs.size() > parameters.pSortFactor) {
		cFinalImmediateMerge += calMergeCost(diskMapOutputs,
			(int) parameters.pSortFactor, memMapOutputs,
			reduceStatistic);
	    }
	}
	System.out.println("cFinalCleanMemToDisk is " + cFinalCleanMemToDisk);
	System.out.println("cFinalImmediateMerge is " + cFinalImmediateMerge);

	double cFinalMerge = calFinalMergeCost(memMapOutputs, diskMapOutputs,
		reduceStatistic) + cFinalCleanMemToDisk + cFinalImmediateMerge;

	double reduceInSizeSum = getReduceInSize(memMapOutputs, diskMapOutputs);
	// run reduce function
	double cReduce = 0;
	cReduce = reduceInSizeSum
		* reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect
		* reduceStatistic.reduceCostStatistics.csReduce;

	double dReduceOutSize = reduceInSizeSum
		* reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect;
	// write to the hdfs
	double cWriteHdfs = 0;
	cWriteHdfs = dReduceOutSize
		* reduceStatistic.reduceCostStatistics.csHdfsWrite;

	System.out.println("cShuffleTime is " + cShuffleTime
		+ " cFinalMerge is " + cFinalMerge + " cReduce is " + cReduce
		+ " cWriteHdfs is " + cWriteHdfs);
	// calcu the reduceTaskCost
	reduceTaskCost = cShuffleTime + cFinalMerge + cReduce + cWriteHdfs;
	System.out.println("the cost of reduce task is " + reduceTaskCost);

	return reduceTaskCost;
    }

    private static double getSizeSum(List<MapOut> memMapOutputs,
	    int numMemToDisk) {
	double sizeSum = 0;
	for (int i = 0; i < numMemToDisk; i++) {
	    sizeSum += memMapOutputs.get(i).mapOutSize;
	}

	return sizeSum;
    }

    public static double[] reduceTaskModel(
	    List<ReduceStatistics> reduceStatistics,
	    ReduceTaskParameters parameters, List<ArrayList<MapOut>> mapOuts) {
	double[] reduceCostsAllNode = new double[reduceStatistics.size()];
	double numReduceTasks = parameters.pNumReduce;

	// count the num of map tasks on the one node
	List<MapOut> mapOutputs = new ArrayList<MapOut>();
	for (ArrayList<MapOut> curMapOuts : mapOuts) {
	    MapOut mapOutput = new MapOut(curMapOuts.get(0).nums
		    * curMapOuts.size(), curMapOuts.get(0).mapCost,
		    curMapOuts.get(0).mapScheduleCost,
		    curMapOuts.get(0).mapOutSize, curMapOuts.get(0).mapOutRecs,
		    curMapOuts.get(0).nodeId);
	    mapOutputs.add(mapOutput);
	}

	// store the num of map tasks each node
	int[] numMapTasksAllNodes = new int[mapOuts.size()];
	for (int i = 0; i < numMapTasksAllNodes.length; i++) {
	    MapOut mapOutput = mapOutputs.get(i);
	    numMapTasksAllNodes[i] = mapOutput.nums;
	}

	// mapoutput for each reduceTask
	for (MapOut mapOut : mapOutputs) {
	    mapOut.mapOutRecs = mapOut.mapOutRecs / numReduceTasks;
	    mapOut.mapOutSize = mapOut.mapOutSize / numReduceTasks;
	}

	for (int i = 0; i < reduceStatistics.size(); i++) {
	    ReduceStatistics reduceStatistic = reduceStatistics.get(i);
	    double reduceTaskCost = 0;

	    // re-initial the mapOutputs'nums
	    for (int j = 0; j < mapOutputs.size(); j++) {
		mapOutputs.get(j).nums = numMapTasksAllNodes[j];
	    }

	    // shuffle ( no inmediate merge)
	    double cShuffleTime = 0;

	    int numParallThreads = (int) parameters.pCopyThread;
	    double usedMemory = 0;
	    boolean isShuffleInMem = mapOutputs.get(0).mapOutSize < parameters.pTotalMemorySize
		    * parameters.pSingleShuffleMemoryLimitPercent;
	    // initial the copyDest
	    int indexDest = 0;
	    Map<Integer, MapOut> copyDest = new HashMap<Integer, MapOut>();
	    for (int j = 0; j < numParallThreads; j++) {
		if (j < mapOutputs.size()) {
		    copyDest.put(j, mapOutputs.get(j));
		    indexDest++;
		} else {
		    copyDest.put(j, null);
		}
	    }

	    List<MapOut> memFinalMapOuts = new ArrayList<MapOut>();
	    List<MapOut> diskFinalMapOuts = new ArrayList<MapOut>();
	    if (isShuffleInMem) {
		double copyThreadClock = 0;
		double memMergeThreadClock = 0;
		double diskMergeThreadClock = 0;
		boolean haveShuffle = false;
		boolean isMerge = false;
		boolean isFull = false;
		List<MapOut> memMapOutputs = new ArrayList<MapOut>();
		List<MapOut> diskMapOutputs = new ArrayList<MapOut>();
		int numMemToDisk = 0;
		while (!isNull(mapOutputs)) {
		    for (int t = 0; t < numParallThreads; t++) {
			MapOut mapoutput = copyDest.get(t);
			if (mapoutput != null) {
			    if (copyThreadClock >= mapoutput.startTime) {
				// copy thread j copy the mapoutput
				mapoutput.nums--;
				memMapOutputs.add(mapoutput);
				haveShuffle = true;
				// after the shuffle
				if (usedMemory < parameters.pTotalMemorySize
					* parameters.pMergeThresholdPercent
					&& usedMemory + mapoutput.mapOutSize >= parameters.pTotalMemorySize
						* parameters.pMergeThresholdPercent) {
				    isMerge = true;
				    numMemToDisk = memMapOutputs.size();
				}
				usedMemory += mapoutput.mapOutSize;
				// if the mapoutput.nums == 0
				if (mapoutput.nums == 0) {
				    if (indexDest < mapOutputs.size()) {
					copyDest.put(t,
						mapOutputs.get(indexDest));
					indexDest++;
				    } else {
					copyDest.put(t, null);
				    }
				}

				// 每次shuffle之后，需要判断缓冲区是否已满！
				if (getMemSize(memMapOutputs) > parameters.pTotalMemorySize) {
				    // 缓冲区满，等待memoryMergeThread清空缓冲区(没有了足够的空间可容纳下一个mapoutput)
				    isFull = true;
				    break;
				}
			    }
			}
		    }
		    if (haveShuffle) {
			// 更新copyThreadClock
			copyThreadClock += mapOutputs.get(0).mapOutSize
				* reduceStatistic.reduceCostStatistics.csShuffleInMem;
			haveShuffle = false;

			if (isMerge) {
			    // 更新memMergeThreadClock
			    memMergeThreadClock = Math.max(memMergeThreadClock,
				    copyThreadClock);
			    memMergeThreadClock += calcuMemoryMergeCost(
				    memMapOutputs, numMemToDisk, parameters,
				    reduceStatistic, diskMapOutputs,
				    memMergeThreadClock);
			    // 需要判断是否需要进行diskfiles merge
			    if (diskMapOutputs.size() >= 2 * parameters.pSortFactor - 1) {
				diskMergeThreadClock = Math.max(
					memMergeThreadClock,
					diskMergeThreadClock); // disk merge
							       // 开始的时间
				diskMergeThreadClock += calcuDiskMergeCost(
					diskMapOutputs, parameters,
					reduceStatistic, diskMergeThreadClock);
			    }

			    isMerge = false;
			}

			if (isFull) {
			    usedMemory -= getSizeSum(memMapOutputs,
				    numMemToDisk);
			    removeMemMergeSegments(memMapOutputs, numMemToDisk);
			    numMemToDisk = 0;
			    // 更新copyThreadClock
			    copyThreadClock = Math.max(memMergeThreadClock,
				    copyThreadClock);
			    isFull = false;
			}

		    } else {
			System.out
				.println("when copy, the reduce task has to wait for the running map tasks!");
			// 更新copyThreadClock
			copyThreadClock = Math.max(copyThreadClock,
				getLastedClock(mapOutputs));
		    }

		}
		cShuffleTime = Math.max(
			Math.max(copyThreadClock, memMergeThreadClock),
			diskMergeThreadClock);
		// remove some of the memMapOutputs
		while (numMemToDisk != 0) {
		    memMapOutputs.remove(0);
		    numMemToDisk--;
		}

		// obtain the final mapoutputs
		for (MapOut mapout : memMapOutputs) {
		    memFinalMapOuts.add(mapout);
		}

		for (MapOut mapout : diskMapOutputs) {
		    diskFinalMapOuts.add(mapout);
		}

	    } else {
		double copyThreadClock = 0;
		double diskThreadClock = 0;
		boolean haveShuffle = false;
		boolean isMerge = false;
		List<ClockSegment> diskClockSegments = new ArrayList<ClockSegment>();
		List<ClockSegment> mergeClockSegments = new ArrayList<ClockSegment>();

		while (!isNull(mapOutputs)) {
		    for (int t = 0; t < numParallThreads; t++) {
			MapOut mapoutput = copyDest.get(t);
			if (mapoutput != null) {
			    if (copyThreadClock >= mapoutput.startTime) {
				// copy thread j copy the mapoutput
				mapoutput.nums--;

				double newCopyThreadClock = copyThreadClock
					+ mapoutput.mapOutSize
					* reduceStatistic.reduceCostStatistics.csShuffleOnDisk;
				ClockSegment diskClock = new ClockSegment(
					mapoutput.mapOutRecs,
					mapoutput.mapOutSize,
					newCopyThreadClock);

				while (mergeClockSegments.size() > 0
					&& mergeClockSegments.get(0).writeClock <= newCopyThreadClock) {
				    ClockSegment mergeClockSegment = mergeClockSegments
					    .remove(0);
				    diskClockSegments.add(mergeClockSegment);
				}

				diskClockSegments.add(diskClock);
				haveShuffle = true;
				if (diskClockSegments.size() >= 2 * parameters.pSortFactor - 1) {
				    isMerge = true;
				}
				if (mapoutput.nums == 0) {
				    if (indexDest < mapOutputs.size()) {
					copyDest.put(t,
						mapOutputs.get(indexDest));
					indexDest++;
				    } else {
					copyDest.put(t, null);
				    }
				}
			    }
			}
		    }
		    if (haveShuffle) {
			// update the copyThreadClock
			copyThreadClock += mapOutputs.get(0).mapOutSize
				* reduceStatistic.reduceCostStatistics.csShuffleOnDisk;
			haveShuffle = false;

			if (isMerge) {
			    diskThreadClock = Math.max(copyThreadClock,
				    diskThreadClock);
			    diskThreadClock += calcuDiskSegmentsMergeCost(
				    diskClockSegments, parameters,
				    reduceStatistic, diskThreadClock,
				    mergeClockSegments);
			}

		    } else {
			System.out
				.println("when copy, the reduce task has to wait for the running map tasks!");
			// 更新copyThreadClock
			copyThreadClock = Math.max(copyThreadClock,
				getLastedClock(mapOutputs));
		    }
		}

		cShuffleTime = Math.max(copyThreadClock, diskThreadClock);

		// obtain the final mapoutputs
		for (ClockSegment segment : diskClockSegments) {
		    MapOut mapout = new MapOut(segment.size, segment.recs);
		    diskFinalMapOuts.add(mapout);
		}

		for (ClockSegment segment : mergeClockSegments) {
		    MapOut mapout = new MapOut(segment.size, segment.recs);
		    diskFinalMapOuts.add(mapout);
		}
	    }

	    // final merge

	    /*
	     * when the final merge starts 1、if the numDiskMapOutputs <
	     * pSortFactor and numMemMapOutputs > 0, then clean and merge the
	     * memMapOutputs into the disk 2、else if the numDiskMapOutputs ==
	     * pSortFactor, then the intermediate merge and clean doesn't occur
	     * 3、 else if the numDiskMapOutputs > pSortFactor, then the
	     * intermediate merge will occur
	     */
	    double cFinalCleanMemToDisk = 0;
	    double cFinalImmediateMerge = 0;
	    if (diskFinalMapOuts.size() < parameters.pSortFactor) {
		if (memFinalMapOuts.size() > 0) {
		    // clean the memMapoutputs into the disk
		    System.out.println("clean the memMapoutputs into the disk");
		    MapOut newDiskFinalMapOutput = new MapOut();
		    cFinalCleanMemToDisk = calCleanMemToDiskCost(
			    memFinalMapOuts, newDiskFinalMapOutput,
			    reduceStatistic);
		    diskFinalMapOuts.add(newDiskFinalMapOutput);
		}
	    } else if (diskFinalMapOuts.size() == parameters.pSortFactor) {
		// no immediate merge
		System.out.println("there is no immediate merge");
	    } else {
		// have immediate merge
		System.out.println("now, it is immediate merging!");
		// first round
		int numDiskMapOuts = diskFinalMapOuts.size();
		int numMapOutsToMergeFirst = (numDiskMapOuts - 1)
			% ((int) parameters.pSortFactor - 1) + 1;

		System.out.println("numDiskMapOut is " + numDiskMapOuts
			+ " numMapOutsToMergeFirst is "
			+ numMapOutsToMergeFirst);
		cFinalImmediateMerge += calMergeCost(diskFinalMapOuts,
			numMapOutsToMergeFirst, memFinalMapOuts,
			reduceStatistic);

		// other rounds
		while (diskFinalMapOuts.size() > parameters.pSortFactor) {
		    cFinalImmediateMerge += calMergeCost(diskFinalMapOuts,
			    (int) parameters.pSortFactor, memFinalMapOuts,
			    reduceStatistic);
		    System.out.println("immediate merge!");
		}
	    }
	    System.out.println("cFinalCleanMemToDisk is "
		    + cFinalCleanMemToDisk);
	    System.out.println("cFinalImmediateMerge is "
		    + cFinalImmediateMerge);

	    double cFinalMerge = calFinalMergeCost(memFinalMapOuts,
		    diskFinalMapOuts, reduceStatistic)
		    + cFinalCleanMemToDisk
		    + cFinalImmediateMerge;

	    double reduceInSizeSum = getReduceInSize(memFinalMapOuts,
		    diskFinalMapOuts);
	    // run reduce function
	    double cReduce = 0;
	    cReduce = reduceInSizeSum
		    * reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect
		    * reduceStatistic.reduceCostStatistics.csReduce;
	    System.out
		    .println("dsReduceNumSelect is "
			    + reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect);

	    double dReduceOutSize = reduceInSizeSum
		    * reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect;
	    System.out
		    .println("reduceInSizeSum is "
			    + reduceInSizeSum
			    + " dsReduceSizeSelect is "
			    + reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect);

	    // write to the hdfs
	    double cWriteHdfs = 0;
	    cWriteHdfs = dReduceOutSize
		    * reduceStatistic.reduceCostStatistics.csHdfsWrite;
	    System.out.println("dReduceOutSize is " + dReduceOutSize
		    + " csHdfsWrite is "
		    + reduceStatistic.reduceCostStatistics.csHdfsWrite);

	    System.out.println("cShuffleTime is " + cShuffleTime
		    + " cFinalMerge is " + cFinalMerge + " cReduce is "
		    + cReduce + " cWriteHdfs is " + cWriteHdfs);
	    // calcu the reduceTaskCost
	    reduceTaskCost = cShuffleTime + cFinalMerge + cReduce + cWriteHdfs;
	    reduceCostsAllNode[i] = reduceTaskCost;
	}
	return reduceCostsAllNode;
    }

    public static MapOut whatIfMapTaskModel(List<MapStatistics> mapStatistics,
	    MapTaskParameters parameters) {
	// calcu the mapStatistic
	MapStatistics mapStatistic = calcuAvgMapStatistic(mapStatistics);

	double mapTaskCost = 0;

	double cTaskInitial = mapStatistic.costStat.csInitialCost; // ms

	// double csReadCost = 10; // ms/MB
	// double cReadTime = parameters.pSplitSize * csReadCost;

	double cReadTime = parameters.pSplitSize * 1024 * 1024
		* mapStatistic.costStat.csReadCost; // ms
	double dMapInputRecs = parameters.pSplitSize * 1024 * 1024
		/ mapStatistic.dataFlowStat.dsInputPairWidth; // total num
							      // input recs
	double cMaptime = 0; // ms
	double dMapOutRecs = dMapInputRecs
		* mapStatistic.dataFlowStat.dsMapRecsSelect;

	double dMaxBufRecs = (parameters.pSortMB * 1024 * 1024 * parameters.pSpillPerc)
		/ mapStatistic.dataFlowStat.dsMapOutRecWidth;
	double dNumSpills = dMapOutRecs / dMaxBufRecs;

	double cSortTime = dMaxBufRecs * mapStatistic.costStat.csSortCost; // ms
	double cCombineReadTime = dMaxBufRecs
		* mapStatistic.costStat.csCombineReadCost; // ms
	double cCombineTime = dMaxBufRecs * mapStatistic.costStat.csCombineCost; // ms
	double dSpillOutRecs = dMaxBufRecs
		* mapStatistic.dataFlowStat.dsCombineRecsSelect;
	double dSpillOutSize = dSpillOutRecs
		* mapStatistic.dataFlowStat.dsSpillOutRecWidth;
	double cCombineWriteTime = dSpillOutRecs
		* mapStatistic.costStat.csCombineWriteCost; // ms
	double cSpillTime = cSortTime + cCombineReadTime + cCombineTime
		+ cCombineWriteTime; // ms

	double dBufRecsLeft = (parameters.pSortMB * 1024 * 1024 * (1 - parameters.pSpillPerc))
		/ mapStatistic.dataFlowStat.dsMapOutRecWidth;

	int dNumSpillImediate = (int) Math.floor(dNumSpills);
	double cOverlapMapSpillAll = 0;
	int numMapsWithSpill = 0;
	int numMapsWithSpillAll = 0;
	if (dNumSpillImediate > 0) {
	    double cMapBufLeftTime = (dBufRecsLeft / mapStatistic.dataFlowStat.dsMapRecsSelect)
		    * mapStatistic.costStat.csMapCostWithSpill;
	    double cOverlapMapSpill = 0;
	    if (cMapBufLeftTime < cSpillTime) {
		cOverlapMapSpill = cMapBufLeftTime;
		numMapsWithSpill = (int) (dBufRecsLeft / mapStatistic.dataFlowStat.dsMapRecsSelect);
	    } else {
		numMapsWithSpill = (int) (cSpillTime / mapStatistic.costStat.csMapCostWithSpill);
		cOverlapMapSpill = cSpillTime;
	    }

	    double dLastSpillImediateMapRecs = Math.min(dMapOutRecs
		    - dNumSpillImediate * dMaxBufRecs, dBufRecsLeft);
	    double cMapSpillLastImediate = (dLastSpillImediateMapRecs / mapStatistic.dataFlowStat.dsMapRecsSelect)
		    * mapStatistic.costStat.csMapCostWithSpill;
	    int numMapsWithLastSpill = 0;
	    double cOverlapMapSpillLast = 0;
	    if (cMapSpillLastImediate < cSpillTime) {
		cOverlapMapSpillLast = cMapSpillLastImediate;
		numMapsWithLastSpill = (int) (dLastSpillImediateMapRecs / mapStatistic.dataFlowStat.dsMapRecsSelect);
	    } else {
		numMapsWithLastSpill = (int) (cSpillTime / mapStatistic.costStat.csMapCostWithSpill);
		cOverlapMapSpillLast = cSpillTime;
	    }

	    cOverlapMapSpillAll = cOverlapMapSpill * (dNumSpillImediate - 1)
		    + cOverlapMapSpillLast;
	    numMapsWithSpillAll = numMapsWithSpill * (dNumSpillImediate - 1)
		    + numMapsWithLastSpill;
	}

	// lastSpill
	double dLastSpillRecs = dMapOutRecs - dNumSpillImediate * dMaxBufRecs;
	double cLastSortTime = dLastSpillRecs
		* mapStatistic.costStat.csSortCost; // ms
	double cLastCombineReadTime = dLastSpillRecs
		* mapStatistic.costStat.csCombineReadCost; // ms
	double cLastCombineTime = dLastSpillRecs
		* mapStatistic.costStat.csCombineCost; // ms
	double dLastSpillOutRecs = dLastSpillRecs
		* mapStatistic.dataFlowStat.dsCombineRecsSelect;
	double dLastSpillOutSize = dLastSpillOutRecs
		* mapStatistic.dataFlowStat.dsSpillOutRecWidth;
	double cLastCombineWriteTime = dLastSpillOutRecs
		* mapStatistic.costStat.csCombineWriteCost; // ms
	double cLastSpillTime = cLastSortTime + cLastCombineReadTime
		+ cLastCombineTime + cLastCombineWriteTime;

	// imediate merge
	double cImediateMerge = 0;
	int dNumSpillReal = dNumSpillImediate + 1;
	boolean haveImediate = dNumSpillReal > parameters.pSortFactor;
	if (haveImediate) {
	    List<Segment> segments = new ArrayList<Segment>();
	    for (int i = 0; i < dNumSpillImediate; i++) {
		Segment segment = new Segment(dSpillOutRecs, dSpillOutSize);
		segments.add(segment);
	    }
	    Segment lastSegment = new Segment(dLastSpillOutRecs,
		    dLastSpillOutSize);
	    segments.add(0, lastSegment);

	    // imediate merge
	    while (segments.size() >= parameters.pSortFactor) {
		List<Segment> toMergeSegments = segments.subList(0,
			(int) parameters.pSortFactor);
		double dMergeSize = 0;
		double dMergeRecs = 0;

		for (Segment segment : toMergeSegments) {
		    dMergeSize += segment.size;
		    dMergeRecs += segment.recs;
		}
		cImediateMerge += dMergeRecs
			* mapStatistic.costStat.csMergeReadCost + dMergeRecs
			* mapStatistic.costStat.csMergeWriteCost; // ms
		Segment newSegment = new Segment(dMergeRecs, dMergeSize);
		segments.add(newSegment);
	    }
	}

	double dTotalMergeSize = dSpillOutSize * dNumSpillImediate
		+ dLastSpillOutSize; // byte
	double dTotalMergeRecs = dSpillOutRecs * dNumSpillImediate
		+ dLastSpillOutRecs; // recs

	double cMergeReadTime = dTotalMergeRecs
		* mapStatistic.costStat.csMergeReadCost; // ms
	double cMergeCombineTime = dTotalMergeRecs
		* mapStatistic.costStat.csMergeCombineCost; // ms
	double dMergeOutRecs = dTotalMergeRecs
		* mapStatistic.dataFlowStat.dsMergeCombineRecsSelect;
	double cMergeWriteTime = dMergeOutRecs
		* mapStatistic.costStat.csMergeWriteCost; // ms

	double cMergeTime = cMergeReadTime + cMergeCombineTime
		+ cMergeWriteTime; // ms

	double cMapTimeNoSpill = (dMapInputRecs - numMapsWithSpillAll)
		* mapStatistic.costStat.csMapCost; //
	double cMapTimeWithSpill = numMapsWithSpillAll
		* mapStatistic.costStat.csMapCostWithSpill; // ms

	cMaptime = cMapTimeNoSpill + cMapTimeWithSpill;
	mapTaskCost = cTaskInitial + cReadTime + cMaptime + cSpillTime
		* dNumSpillImediate + cLastSpillTime + cMergeTime
		+ cImediateMerge; // ms

	MapOut mapOutput = new MapOut();
	mapOutput.mapCost = mapTaskCost;
	mapOutput.mapOutRecs = dMergeOutRecs;
	mapOutput.mapOutSize = dTotalMergeSize
		* mapStatistic.dataFlowStat.dsMergeCombineRecsSelect;

	System.out.println(" the map cost of whatIf Model is "
		+ mapOutput.mapCost);

	return mapOutput;
    }

    public static double[] mapTaskModel(MapStatistics mapStatistics,
	    MapTaskParameters parameters) {
	double[] out = new double[4]; // cost、 kv 、size

	double mapTaskCost = 0;

	double cTaskInitial = mapStatistics.costStat.csInitialCost;
	double cReadTime = mapStatistics.costStat.csReadCost
		* parameters.pSplitSize * 1024 * 1024; // ms

	double dMapInputRecs = parameters.pSplitSize * 1024 * 1024
		/ mapStatistics.dataFlowStat.dsInputPairWidth; // total num
							       // input recs
	double cMaptime = 0; // ms
	double dMapOutRecs = dMapInputRecs
		* mapStatistics.dataFlowStat.dsMapRecsSelect;

	double dMaxBufRecs = (parameters.pSortMB * 1024 * 1024 * parameters.pSpillPerc)
		/ mapStatistics.dataFlowStat.dsMapOutRecWidth;
	double dNumSpills = dMapOutRecs / dMaxBufRecs;
	double cSortTime = dMaxBufRecs * mapStatistics.costStat.csSortCost; // ms

	double cCombineReadTime = dMaxBufRecs
		* mapStatistics.costStat.csCombineReadCost; // ms
	double cCombineTime = dMaxBufRecs
		* mapStatistics.costStat.csCombineCost; // ms
	double dSpillOutRecs = dMaxBufRecs
		* mapStatistics.dataFlowStat.dsCombineRecsSelect;
	double dSpillOutSize = dSpillOutRecs
		* mapStatistics.dataFlowStat.dsSpillOutRecWidth;
	double cCombineWriteTime = dSpillOutRecs
		* mapStatistics.costStat.csCombineWriteCost; // ms
	double cSpillTime = cSortTime + cCombineReadTime + cCombineTime
		+ cCombineWriteTime; // ms

	double dBufRecsLeft = (parameters.pSortMB * 1024 * 1024 * (1 - parameters.pSpillPerc))
		/ mapStatistics.dataFlowStat.dsMapOutRecWidth;

	int dNumSpillImediate = (int) Math.floor(dNumSpills);
	double cOverlapMapSpillAll = 0;
	int numMapsWithSpill = 0;
	int numMapsWithSpillAll = 0;
	if (dNumSpillImediate > 0) {
	    double cMapBufLeftTime = (dBufRecsLeft / mapStatistics.dataFlowStat.dsMapRecsSelect)
		    * mapStatistics.costStat.csMapCostWithSpill;
	    double cOverlapMapSpill = 0;
	    if (cMapBufLeftTime < cSpillTime) {
		cOverlapMapSpill = cMapBufLeftTime;
		numMapsWithSpill = (int) (dBufRecsLeft / mapStatistics.dataFlowStat.dsMapRecsSelect);
	    } else {
		numMapsWithSpill = (int) (cSpillTime / mapStatistics.costStat.csMapCostWithSpill);
		cOverlapMapSpill = cSpillTime;
	    }

	    double dLastSpillImediateMapRecs = Math.min(dMapOutRecs
		    - dNumSpillImediate * dMaxBufRecs, dBufRecsLeft); // 有点问题
	    double cMapSpillLastImediate = (dLastSpillImediateMapRecs / mapStatistics.dataFlowStat.dsMapRecsSelect)
		    * mapStatistics.costStat.csMapCostWithSpill;
	    int numMapsWithLastSpill = 0;
	    double cOverlapMapSpillLast = 0;
	    if (cMapSpillLastImediate < cSpillTime) {
		cOverlapMapSpillLast = cMapSpillLastImediate;
		numMapsWithLastSpill = (int) (dLastSpillImediateMapRecs / mapStatistics.dataFlowStat.dsMapRecsSelect);
	    } else {
		numMapsWithLastSpill = (int) (cSpillTime / mapStatistics.costStat.csMapCostWithSpill);
		cOverlapMapSpillLast = cSpillTime;
	    }

	    cOverlapMapSpillAll = cOverlapMapSpill * (dNumSpillImediate - 1)
		    + cOverlapMapSpillLast;
	    numMapsWithSpillAll = numMapsWithSpill * (dNumSpillImediate - 1)
		    + numMapsWithLastSpill;
	}

	// lastSpill
	double dLastSpillRecs = dMapOutRecs - dNumSpillImediate * dMaxBufRecs;
	double cLastSortTime = dLastSpillRecs
		* mapStatistics.costStat.csSortCost; // ms
	double cLastCombineReadTime = dLastSpillRecs
		* mapStatistics.costStat.csCombineReadCost; // ms
	double cLastCombineTime = dLastSpillRecs
		* mapStatistics.costStat.csCombineCost; // ms
	double dLastSpillOutRecs = dLastSpillRecs
		* mapStatistics.dataFlowStat.dsCombineRecsSelect;
	double dLastSpillOutSize = dLastSpillOutRecs
		* mapStatistics.dataFlowStat.dsSpillOutRecWidth;
	double cLastCombineWriteTime = dLastSpillOutRecs
		* mapStatistics.costStat.csCombineWriteCost; // ms
	double cLastSpillTime = cLastSortTime + cLastCombineReadTime
		+ cLastCombineTime + cLastCombineWriteTime;

	// imediate merge, there is no combining when imediate merge
	double cImediateMerge = 0;
	int dNumSpillReal = dNumSpillImediate + 1;
	boolean haveImediate = dNumSpillReal > parameters.pSortFactor;
	if (haveImediate) {
	    List<Segment> segments = new ArrayList<Segment>();
	    for (int i = 0; i < dNumSpillImediate; i++) {
		Segment segment = new Segment(dSpillOutRecs, dSpillOutSize);
		segments.add(segment);
	    }
	    Segment lastSegment = new Segment(dLastSpillOutRecs,
		    dLastSpillOutSize);
	    segments.add(0, lastSegment);

	    // imediate merge
	    while (segments.size() >= parameters.pSortFactor) {
		List<Segment> toMergeSegments = segments.subList(0,
			(int) parameters.pSortFactor);
		double dMergeSize = 0;
		double dMergeRecs = 0;

		for (Segment segment : toMergeSegments) {
		    dMergeSize += segment.size;
		    dMergeRecs += segment.recs;
		}
		cImediateMerge += dMergeRecs
			* mapStatistics.costStat.csMergeReadCost + dMergeRecs
			* mapStatistics.costStat.csMergeWriteCost; // ms
		Segment newSegment = new Segment(dMergeRecs, dMergeSize);
		segments.add(newSegment);
	    }
	}

	// Final Merge
	double dTotalMergeSize = dSpillOutSize * dNumSpillImediate
		+ dLastSpillOutSize; // byte
	double dTotalMergeRecs = dSpillOutRecs * dNumSpillImediate
		+ dLastSpillOutRecs; // recs

	double cMergeReadTime = dTotalMergeRecs
		* mapStatistics.costStat.csMergeReadCost; // ms
	double cMergeCombineTime = dTotalMergeRecs
		* mapStatistics.costStat.csMergeCombineCost; // ms
	double dMergeOutRecs = dTotalMergeRecs
		* mapStatistics.dataFlowStat.dsMergeCombineRecsSelect;
	System.out.println("dTotalMergeRecs is " + dTotalMergeRecs
		+ " and dsMergeCombineRecsSelect is "
		+ mapStatistics.dataFlowStat.dsMergeCombineRecsSelect);

	double cMergeWriteTime = dMergeOutRecs
		* mapStatistics.costStat.csMergeWriteCost; // ms

	double cMergeTime = cMergeReadTime + cMergeCombineTime
		+ cMergeWriteTime; // ms

	double cMapTimeNoSpill = (dMapInputRecs - numMapsWithSpillAll)
		* mapStatistics.costStat.csMapCost; //
	double cMapTimeWithSpill = numMapsWithSpillAll
		* mapStatistics.costStat.csMapCostWithSpill; // ms
	cMaptime = cMapTimeNoSpill + cMapTimeWithSpill;
	System.out.println("cMapTimeNoSpill is " + cMapTimeNoSpill
		+ " cMapTimeWithSpill is " + cMapTimeWithSpill);
	System.out.println("numMapsWithSpillAll is " + numMapsWithSpillAll
		+ " csMapCostWithSpill  is "
		+ mapStatistics.costStat.csMapCostWithSpill);

	mapTaskCost = cTaskInitial + cReadTime + cMaptime + cSpillTime
		* dNumSpillImediate + cLastSpillTime - cOverlapMapSpillAll
		+ cMergeTime + cImediateMerge; // ms
	System.out.println("cTaskInitial is " + cTaskInitial + " cReadTime is "
		+ cReadTime + " cMaptime is " + cMaptime + "  cSpillTime is "
		+ cSpillTime + " cLastSpillTime is " + cLastSpillTime
		+ " cMergeTime " + cMergeTime + " cImediateMerge is "
		+ cImediateMerge);
	System.out.println("cOverlapMapSpillAll is " + cOverlapMapSpillAll);

	// out[2] may have problem
	out[0] = mapTaskCost;
	out[1] = dMergeOutRecs;
	out[2] = dTotalMergeSize
		* mapStatistics.dataFlowStat.dsMergeCombineRecsSelect;
	out[3] = mapStatistics.costStat.csScheduleCost;

	return out;
    }

    public double jobCost(List<Statistics> statistics,
	    MapTaskParameters mapParameters,
	    ReduceTaskParameters reduceParameters, int inputSize,
	    ClusterConf clusterConf) {
	List<MapStatistics> mapStatistics = new ArrayList<MapStatistics>();
	List<ReduceStatistics> reduceStatistics = new ArrayList<ReduceStatistics>();
	for (Statistics statistic : statistics) {
	    if (statistic.isEmpty)
		continue;
	    System.out.println(statistic.hostName);

	    if (statistic.mapStatistics != null) {
		mapStatistics.add(statistic.mapStatistics);
	    }

	    if (statistic.reduceStatistics != null) {
		reduceStatistics.add(statistic.reduceStatistics);
	    }
	}

	int numNodesNeeded = (int) (inputSize / (clusterConf.numMapsOneNode * mapParameters.pSplitSize));
	double mapWaves = Math.ceil((numNodesNeeded * 1.0)
		/ clusterConf.numSlaves);

	List<MapOut> mapOutsAllNode = new ArrayList<MapOut>();
	for (int i = 0; i < mapStatistics.size(); i++) {
	    double[] mapOut = mapTaskModel(mapStatistics.get(i), mapParameters);
	    MapOut mapOutObj = new MapOut();
	    mapOutObj.mapCost = mapOut[0];
	    mapOutObj.mapOutRecs = mapOut[1];
	    mapOutObj.mapOutSize = mapOut[2];
	    mapOutObj.mapScheduleCost = mapOut[3];
	    mapOutObj.nodeId = i;

	    mapOutsAllNode.add(mapOutObj);
	}

	// sort the mapOutsAllNode, mapOut should record the i
	Collections.sort(mapOutsAllNode);

	List<ArrayList<MapOut>> mapOutsAll = new ArrayList<ArrayList<MapOut>>(); // multiple
										 // waves
										 // map
										 // tasks
	for (int i = 0; i < clusterConf.numSlaves; i++) {
	    ArrayList<MapOut> mapOutsOneNode = new ArrayList<MapOut>();
	    mapOutsAll.add(mapOutsOneNode);
	}

	// construct the mapOuts for the input of reduce task
	while (numNodesNeeded > 0) {
	    for (int i = 0; i < clusterConf.numSlaves; i++) {
		MapOut newMapOut = new MapOut(clusterConf.numMapsOneNode,
			mapOutsAllNode.get(i).mapCost,
			mapOutsAllNode.get(i).mapScheduleCost,
			mapOutsAllNode.get(i).mapOutSize,
			mapOutsAllNode.get(i).mapOutRecs,
			mapOutsAllNode.get(i).nodeId);
		mapOutsAll.get(i).add(newMapOut);
		numNodesNeeded--;
		if (numNodesNeeded <= 0)
		    break;
	    }
	}

	int numReduceTasks = (int) reduceParameters.pNumReduce;
	double reduceWaves = Math.ceil((numReduceTasks * 1.0)
		/ (clusterConf.numSlaves * clusterConf.numReducesOneNode));

	// calcu the cost of the reduce tasks of all the machines
	double[] reduceCosts = reduceTaskModel(reduceStatistics,
		reduceParameters, mapOutsAll);

	// we calcu the job costs
	// we assumes that numSlaves == mapStatistics.size() ==
	// reduceStatistics.size()
	double[] jobCostsAllNode = new double[clusterConf.numSlaves];
	for (int i = 0; i < clusterConf.numSlaves; i++) {
	    ArrayList<MapOut> mapOutsOneSlave = mapOutsAll.get(i); // the amount
								   // of this
								   // mapOutsOneSlave
	    int reduceId = mapOutsOneSlave.get(0).nodeId;

	    jobCostsAllNode[i] = (mapOutsOneSlave.get(0).mapCost + mapOutsOneSlave
		    .get(0).mapScheduleCost)
		    * mapOutsOneSlave.size()
		    + (reduceCosts[reduceId] + reduceStatistics.get(reduceId).reduceCostStatistics.csScheduleCost)
		    * reduceWaves;

	}

	// the cost of the job is the maximum value of the all the nodes' job
	// costs(jobCostsAllNode)
	double newModelJobCost = 0;
	for (double jobCost : jobCostsAllNode) {
	    if (newModelJobCost < jobCost)
		newModelJobCost = jobCost;
	}

	return newModelJobCost;
    }

    public static void main(String[] args) {
	if (args.length == 0) {
	    System.out.println("please specify the jobId");
	}

	String path = args[0] + ".txt";

	List<Statistics> statistics = null;

	try {
	    ObjectInputStream is = new ObjectInputStream(new FileInputStream(
		    path));
	    statistics = (ArrayList<Statistics>) is.readObject();

	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	List<MapStatistics> mapStatistics = new ArrayList<MapStatistics>();
	List<ReduceStatistics> reduceStatistics = new ArrayList<ReduceStatistics>();
	for (Statistics statistic : statistics) {
	    if (statistic.isEmpty)
		continue;
	    System.out.println(statistic.hostName);

	    if (statistic.mapStatistics != null) {
		mapStatistics.add(statistic.mapStatistics);
	    }

	    if (statistic.reduceStatistics != null) {
		reduceStatistics.add(statistic.reduceStatistics);
	    }
	}
	int numMapStatis = mapStatistics.size();
	int numReduceStatis = reduceStatistics.size();
	System.out.println("the number of mapStatis is " + numMapStatis);
	System.out.println("the number of reduceStatis is " + numReduceStatis);

	// initial the parameter of mapTask
	MapTaskParameters mapParameters = new MapTaskParameters();
	mapParameters.pSplitSize = 128;
	mapParameters.pSortMB = 100;
	mapParameters.pSpillPerc = 0.8;
	mapParameters.pIsCombine = true;
	mapParameters.pSortFactor = 10;
	mapParameters.pNumSpillForComb = 3;

	int inputSize = 94208; // 92GB
	int numSlaves = 14; // the number of slaves is the number of
			    // mapStatistics
	int numMapsOneNode = 16; // 16 maps per node
	int numNodesNeeded = (int) (inputSize / (numMapsOneNode * mapParameters.pSplitSize));
	double mapWaves = Math.ceil((numNodesNeeded * 1.0) / numSlaves);

	// new model
	List<MapOut> mapOutsAllNode = new ArrayList<MapOut>(); // 记录每台机器上每一个mapTask的输出、开销
	for (int i = 0; i < mapStatistics.size(); i++) {
	    double[] mapOut = mapTaskModel(mapStatistics.get(i), mapParameters);
	    MapOut mapOutObj = new MapOut();
	    mapOutObj.mapCost = mapOut[0];
	    mapOutObj.mapOutRecs = mapOut[1];
	    mapOutObj.mapOutSize = mapOut[2];
	    mapOutObj.mapScheduleCost = mapOut[3];
	    mapOutObj.nodeId = i;

	    mapOutsAllNode.add(mapOutObj);

	    System.out.println("the map cost of the node number " + i + " is "
		    + mapOutObj.mapCost);
	    System.out.println("the recs of the output " + i + " is "
		    + mapOutObj.mapOutRecs);
	    System.out.println("the size of the output " + i + " is "
		    + mapOutObj.mapOutSize);
	}

	// sort the mapOutsAllNode, mapOut should record the i
	Collections.sort(mapOutsAllNode);

	List<ArrayList<MapOut>> mapOutsAll = new ArrayList<ArrayList<MapOut>>(); // multiple
										 // waves
										 // map
										 // tasks
	for (int i = 0; i < numSlaves; i++) {
	    ArrayList<MapOut> mapOutsOneNode = new ArrayList<MapOut>();
	    mapOutsAll.add(mapOutsOneNode);
	}

	// construct the mapOuts for the input of reduce task
	while (numNodesNeeded > 0) {
	    for (int i = 0; i < numSlaves; i++) {
		MapOut newMapOut = new MapOut(numMapsOneNode,
			mapOutsAllNode.get(i).mapCost,
			mapOutsAllNode.get(i).mapScheduleCost,
			mapOutsAllNode.get(i).mapOutSize,
			mapOutsAllNode.get(i).mapOutRecs,
			mapOutsAllNode.get(i).nodeId);
		mapOutsAll.get(i).add(newMapOut);
		numNodesNeeded--;
		if (numNodesNeeded <= 0)
		    break;
	    }
	}

	// initial the parameter of reduceTask
	ReduceTaskParameters reduceParameters = new ReduceTaskParameters();
	reduceParameters.pCopyThread = 5;
	reduceParameters.pNumReduce = 224;
	reduceParameters.pReduceInBufPerc = 0;
	reduceParameters.pHaveCombiner = false;
	reduceParameters.pSingleShuffleMemoryLimitPercent = 0.25;
	reduceParameters.pTotalMemorySize = 140 * 1024 * 1024; // byte
	reduceParameters.pSortFactor = 10;
	reduceParameters.pMergeThresholdPercent = 0.625; // not 0.9

	int numMapTasks = (int) (inputSize / mapParameters.pSplitSize);
	int numReduceTasks = (int) reduceParameters.pNumReduce;
	int numReducesOneNode = 16;
	double reduceWaves = Math.ceil((numReduceTasks * 1.0)
		/ (numSlaves * numReducesOneNode));

	double[] reduceCosts = reduceTaskModel(reduceStatistics,
		reduceParameters, mapOutsAll);
	for (int i = 0; i < reduceCosts.length; i++)
	    System.out.println("the reduce cost of reduce task " + i + " is "
		    + reduceCosts[i]);
	// we assumes that numSlaves == mapStatistics.size() ==
	// reduceStatistics.size()
	double[] jobCostsAllNode = new double[numSlaves];
	for (int i = 0; i < numSlaves; i++) {
	    ArrayList<MapOut> mapOutsOneSlave = mapOutsAll.get(i); // the amount
								   // of this
								   // mapOutsOneSlave
	    int reduceId = mapOutsOneSlave.get(0).nodeId;

	    jobCostsAllNode[i] = (mapOutsOneSlave.get(0).mapCost + mapOutsOneSlave
		    .get(0).mapScheduleCost)
		    * mapOutsOneSlave.size()
		    + (reduceCosts[reduceId] + reduceStatistics.get(reduceId).reduceCostStatistics.csScheduleCost)
		    * reduceWaves;

	}

	// the cost of job is the maximum value of all the nodes' job
	// costs(jobCostsAllNode)
	double newModelJobCost = 0;
	for (double jobCost : jobCostsAllNode) {
	    if (newModelJobCost < jobCost)
		newModelJobCost = jobCost;
	}

	System.out.println("the job cost of the new model is "
		+ newModelJobCost);

	// whatIf model
	MapOut mapOut = whatIfMapTaskModel(mapStatistics, mapParameters);

	double reduceCost = whatIfReduceTaskModel(reduceStatistics,
		reduceParameters, mapOut, numMapTasks, numReduceTasks);
	double whatIfModelJobCost = mapOut.mapCost + reduceCost;
	System.out.println("the job cost of the whatIf model is "
		+ whatIfModelJobCost);

    }

    private static ReduceStatistics calcuAvgReduceStatistic(
	    List<ReduceStatistics> reduceStatistics) {
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
	    tempStatistic.reduceCostStatistics.csImeMemToDiskMerge += statistics.reduceCostStatistics.csImeMemToDiskMerge;
	    tempStatistic.reduceCostStatistics.csImeMemToDiskCombine += statistics.reduceCostStatistics.csImeMemToDiskCombine;
	    tempStatistic.reduceCostStatistics.csImeMemToDiskWrite += statistics.reduceCostStatistics.csImeMemToDiskWrite;
	    tempStatistic.reduceCostStatistics.csImeDiskToDiskMerge += statistics.reduceCostStatistics.csImeDiskToDiskMerge;
	    tempStatistic.reduceCostStatistics.csImeDiskToDiskWrite += statistics.reduceCostStatistics.csImeDiskToDiskWrite;

	    tempStatistic.reduceCostStatistics.csFinalMemToDiskMerge += statistics.reduceCostStatistics.csFinalMemToDiskMerge;
	    tempStatistic.reduceCostStatistics.csFinalMemToDiskWrite += statistics.reduceCostStatistics.csFinalMemToDiskWrite;
	    tempStatistic.reduceCostStatistics.csFinalDiskToDiskMerge += statistics.reduceCostStatistics.csFinalDiskToDiskMerge;
	    tempStatistic.reduceCostStatistics.csReduce += statistics.reduceCostStatistics.csReduce;
	    tempStatistic.reduceCostStatistics.csHdfsWrite += statistics.reduceCostStatistics.csHdfsWrite;

	    tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect += statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineSizeSelect;
	    tempStatistic.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect += statistics.reduceDataFlowStatistics.dsShuffleMemToDiskCombineRecsSelect;
	    tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect += statistics.reduceDataFlowStatistics.dsMemToDiskCombineSizeSelect;
	    tempStatistic.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect += statistics.reduceDataFlowStatistics.dsMemToDiskCombineRecsSelect;
	    tempStatistic.reduceDataFlowStatistics.dsReduceNumSelect += statistics.reduceDataFlowStatistics.dsReduceNumSelect;
	    tempStatistic.reduceDataFlowStatistics.dsReduceSizeSelect += statistics.reduceDataFlowStatistics.dsReduceSizeSelect;

	}

	int nums = reduceStatistics.size();
	ReduceStatistics reduceStatistic = new ReduceStatistics();
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
	reduceStatistic.reduceDataFlowStatistics.dsReduceNumSelect = tempStatistic.reduceDataFlowStatistics.dsReduceNumSelect
		/ nums;
	reduceStatistic.reduceDataFlowStatistics.dsReduceSizeSelect = tempStatistic.reduceDataFlowStatistics.dsReduceSizeSelect
		/ nums;

	return reduceStatistic;
    }

    private static MapStatistics calcuAvgMapStatistic(
	    List<MapStatistics> mapStatistics) {
	MapStatistics tempStatistic = new MapStatistics();
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

	MapStatistics mapStatistic = new MapStatistics();
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

	return mapStatistic;
    }
}

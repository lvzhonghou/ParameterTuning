package com.lvzhonghou.Optimizer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import com.lvzhonghou.Prophet.ClusterConf;
import com.lvzhonghou.Prophet.MapTaskParameters;
import com.lvzhonghou.Prophet.PerformanceModel;
import com.lvzhonghou.Prophet.ReduceTaskParameters;
import com.lvzhonghou.StatisticsEstimate.StatisticsPredictor;
import com.lvzhonghou.common.MapCostStatistics;
import com.lvzhonghou.common.MapStatistics;
import com.lvzhonghou.common.ReduceCostStatistics;
import com.lvzhonghou.common.ReduceStatistics;
import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年7月31日 下午5:20:05
 * @version v1。0
 */
public class JobOptimizer {
    public MapTaskParameters mapParameters;
    public ReduceTaskParameters reduceParameters;

    public JobOptimizer() {
	mapParameters = new MapTaskParameters();
	reduceParameters = new ReduceTaskParameters();
    }
    
    // obtain the iteration cells
    public IterationCell[] getIterationCells(int inputSize, int tradeoffParas,
	    ClusterConf clusterConf) {
	IterationCell[] iterations = new IterationCell[tradeoffParas];
	iterations[0] = new IterationCell(); // pSplitSize
	iterations[0].start = 64; // MB
	iterations[0].interval = 16;
	iterations[0].end = Math.min(400.0, inputSize
		/ (clusterConf.numMapsOneNode * clusterConf.numSlaves));
	iterations[1] = new IterationCell(); // pSpillPerc
	iterations[1].start = 0.5; // percent
	iterations[1].interval = 0.05;
	iterations[1].end = 0.9;
	iterations[2] = new IterationCell(); // pSortFactor
	iterations[2].start = 5;
	iterations[2].interval = 1;
	iterations[2].end = 30;
	iterations[3] = new IterationCell(); // pNumSpillForComb
	iterations[3].start = 2;
	iterations[3].interval = 1;
	iterations[3].end = 15;
	iterations[4] = new IterationCell(); // pSingleShuffleMemoryLimitPercent
	iterations[4].start = 0.10;
	iterations[4].interval = 0.01;
	iterations[4].end = 0.40;
	iterations[5] = new IterationCell(); // pMergeThresholdPercent
	iterations[5].start = 0.6;
	iterations[5].interval = 0.01;
	iterations[5].end = 0.95;
	iterations[6] = new IterationCell(); // pCopyThread
	iterations[6].start = 1;
	iterations[6].interval = 1;
	iterations[6].end = 14;
	iterations[7] = new IterationCell(); // pNumReduce
	iterations[7].start = clusterConf.numReducesOneNode
		* clusterConf.numSlaves;
	iterations[7].interval = clusterConf.numReducesOneNode
		* clusterConf.numSlaves;
	iterations[7].end = clusterConf.numReducesOneNode
		* clusterConf.numSlaves * 6;

	return iterations;
    }
    
    // print the optimization result
    public static void printOpt(MapTaskParameters mapOpt, ReduceTaskParameters reduceOpt) {
	// print the optimized parameters
	System.out.println("the optimized parameters are shown as follows: ");
	System.out.println("mapTask's pSplitSize is "
		+ mapOpt.pSplitSize);
	System.out.println("mapTask's pSpillPerc is "
		+ mapOpt.pSpillPerc);
	System.out.println("mapTask's pSortFactor is "
		+ mapOpt.pSortFactor);
	System.out.println("mapTask's pNumSpillForComb is "
		+ mapOpt.pNumSpillForComb);
	System.out.println("reduceTask's pSortFactor is "
		+ reduceOpt.pSortFactor);
	System.out.println("reduceTask's pSingleShuffleMemoryLimitPercent is "
		+ reduceOpt.pSingleShuffleMemoryLimitPercent);
	System.out.println("reduceTask's pMergeThresholdPercent is "
		+ reduceOpt.pMergeThresholdPercent);
	System.out.println("reduceTask's pCopyThread is "
		+ reduceOpt.pCopyThread);
	System.out.println("reduceTask's pNumReduce is "
		+ reduceOpt.pNumReduce);
    }
    
    // print the statistics predicted by the predictor 
    public static void printStatis(Statistics statis) {
	System.out.println("the predicted statistics are shown as follows: ");
	
	String host = statis.hostName;
	System.out.println("the host name is " + host);
	
	MapStatistics mapStatis = statis.mapStatistics;
	ReduceStatistics reduceStatis = statis.reduceStatistics;
	
	MapCostStatistics mapCostStatis = mapStatis.costStat;
	ReduceCostStatistics reduceCostStatis = reduceStatis.reduceCostStatistics;
	
	System.out.println("the map cost statistics are shown as follows.");
	System.out.println("the map's csScheduleCost is " + mapCostStatis.csScheduleCost);
	System.out.println("the map's  csInitialCost is " + mapCostStatis.csInitialCost);
	System.out.println("the map's hdfsReadRate is " + mapCostStatis.hdfsReadRate);
	System.out.println("the map's ioReadRate is " + mapCostStatis.ioReadRate);
	System.out.println("the map's csReadCost is " + mapCostStatis.csReadCost);
	System.out.println("the map's csMapCost is " + mapCostStatis.csMapCost);
	System.out.println("the map's csMapCostWithSpill is " + mapCostStatis.csMapCostWithSpill);
	System.out.println("the map's csSortCost is " + mapCostStatis.csSortCost);
	System.out.println("the map's csCombineCost is " + mapCostStatis.csCombineCost);
	System.out.println("the map's csCombineReadCost is " + mapCostStatis.csCombineReadCost);
	System.out.println("the map's csCombineWriteCost is " + mapCostStatis.csCombineWriteCost);
	System.out.println("the map's csMergeReadCost is " + mapCostStatis.csMergeReadCost);
	System.out.println("the map's csMergeCombineCost is " + mapCostStatis.csMergeCombineCost);
	System.out.println("the map's csMergeWriteCost is " + mapCostStatis.csMergeWriteCost);
	
	
	System.out.println("-------------------------------");
	System.out.println("the reduce cost statistics are shown as follows.");
	System.out.println("the reduce's csSheduleCost is " + reduceCostStatis.csScheduleCost);
	System.out.println("the reduce's initialCost is " + reduceCostStatis.initialCost);
	System.out.println("the reduce's csShuffleInMem is " + reduceCostStatis.csShuffleInMem);
	System.out.println("the reduce's csShuffleOnDisk is " + reduceCostStatis.csShuffleOnDisk);
	System.out.println("the reduce's csShuffleMemToDiskMerge is " + reduceCostStatis.csShuffleMemToDiskMerge);
	System.out.println("the reduce's csShuffleMemToDiskCombine is " + reduceCostStatis.csShuffleMemToDiskCombine);
	System.out.println("the reduce's csShuffleMemToDiskWrite is " + reduceCostStatis.csShuffleMemToDiskWrite);
	System.out.println("the reduce's csShuffleDiskToDiskMerge is " + reduceCostStatis.csShuffleDiskToDiskMerge);
	System.out.println("the reduce's csShuffleDiskToDiskWrite is " + reduceCostStatis.csShuffleDiskToDiskWrite);
	System.out.println("the reduce's csImeMemToDiskMerge is " + reduceCostStatis.csImeMemToDiskMerge);
	System.out.println("the reduce's csImeMemToDiskCombine is " + reduceCostStatis.csImeMemToDiskCombine);
	System.out.println("the reduce's csImeMemToDiskWrite is " + reduceCostStatis.csFinalMemToDiskWrite);
	System.out.println("the reduce's csImeDiskToDiskMerge is " + reduceCostStatis.csImeDiskToDiskMerge);
	System.out.println("the reduce's csImeDiskToDiskWrite is " + reduceCostStatis.csImeDiskToDiskWrite);
	System.out.println("the reduce's csFinalMemToDiskMerge is " + reduceCostStatis.csFinalMemToDiskMerge);
	System.out.println("the reduce's csFinalMemToDiskWrite is " + reduceCostStatis.csFinalMemToDiskWrite);
	System.out.println("the reduce's csFinalDiskToDiskMerge is " + reduceCostStatis.csFinalDiskToDiskMerge);
	System.out.println("the reduce's csReduce is " + reduceCostStatis.csReduce);
	System.out.println("the reduce's csHdfsWrite is " + reduceCostStatis.csHdfsWrite);
    }
    
    
    
    /**
     * @Description
     * @param args
     */
    public static void main(String[] args) {
	// read the statistics from the various files
	if (args.length == 0) {
	    System.out.println("please specify the jobId");
	}

	String[] paths = new String[args.length];
	for (int i = 0; i < args.length; i++)
	    paths[i] = args[i] + ".txt";

	// jobsStatis represents all the statistics of various files, one
	// element of the jobsStatis is one job's statistics
	List<ArrayList<Statistics>> jobsStatis = new ArrayList<ArrayList<Statistics>>();
	try {
	    for (String path : paths) {
		ObjectInputStream is = new ObjectInputStream(
			new FileInputStream(path));
		ArrayList<Statistics> statistics = (ArrayList<Statistics>) is
			.readObject();
		jobsStatis.add(statistics);
	    }
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

	// reset the jobsStatis, all the jobsStatis of the same hostName are set
	// in the same list
	List<ArrayList<Statistics>> hostsStatis = new ArrayList<ArrayList<Statistics>>();
	for (Statistics statis : jobsStatis.get(0)) {
	    ArrayList<Statistics> hostStatis = new ArrayList<Statistics>();
	    hostStatis.add(statis);
	    hostsStatis.add(hostStatis);
	}
	for (int i = 1; i < jobsStatis.size(); i++) {
	    if (i >= jobsStatis.size())
		break;

	    // traverse the statisArrs
	    for (Statistics statis : jobsStatis.get(i)) {
		String hostName = statis.hostName;
		for (ArrayList<Statistics> hostStatis : hostsStatis) {
		    if (hostName.equals(hostStatis.get(0).hostName)) {
			hostStatis.add(statis);
			break;
		    }
		}
	    }
	}

	// init the StatisticsPredictor
	StatisticsPredictor statisticsPredictor = new StatisticsPredictor();

	// init the PerformanceModel
	PerformanceModel performanceModel = new PerformanceModel();

	JobOptimizer jobOptimizer = new JobOptimizer();
	int inputSize = 100 * 1024; // MB

	// fix some parameters
	jobOptimizer.mapParameters.pIsCombine = true; // this parameter can not
						      // be specified by the
						      // user, but determined by
						      // the program

	// maximize the paramters related to resource as much as possible
	jobOptimizer.mapParameters.pSortMB = 100;
	jobOptimizer.reduceParameters.pTotalMemorySize = 140 * 1024 * 1024; // byte
	jobOptimizer.reduceParameters.pReduceInBufPerc = 0;

	// init the clusterConf
	ClusterConf clusterConf = new ClusterConf();
	clusterConf.numMapsOneNode = 16;
	clusterConf.numReducesOneNode = 16;
	clusterConf.numSlaves = 14;

	// brute search
	// determine the start / end / interval
	int tradeoffParas = 8;
	IterationCell[] iterations = jobOptimizer.getIterationCells(inputSize,
		tradeoffParas, clusterConf);

	double minJobCost = 0;
	MapTaskParameters mapParametersOpt = new MapTaskParameters();
	ReduceTaskParameters reduceParametersOpt = new ReduceTaskParameters();
	
	int iterationNums = 0;
	for (jobOptimizer.mapParameters.pSplitSize = iterations[0].start; jobOptimizer.mapParameters.pSplitSize <= iterations[0].end; jobOptimizer.mapParameters.pSplitSize += iterations[0].interval) {
	    for (jobOptimizer.mapParameters.pSpillPerc = iterations[1].start; jobOptimizer.mapParameters.pSpillPerc <= iterations[1].end; jobOptimizer.mapParameters.pSpillPerc += iterations[1].interval) {
		for (jobOptimizer.mapParameters.pSortFactor = iterations[2].start; jobOptimizer.mapParameters.pSortFactor <= iterations[2].end; jobOptimizer.mapParameters.pSortFactor += iterations[2].interval) {
		    jobOptimizer.reduceParameters.pSortFactor = jobOptimizer.mapParameters.pSortFactor;
		    for (jobOptimizer.mapParameters.pNumSpillForComb = iterations[3].start; jobOptimizer.mapParameters.pNumSpillForComb <= iterations[3].end; jobOptimizer.mapParameters.pNumSpillForComb += iterations[3].interval) {
			for (jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent = iterations[4].start; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent <= iterations[4].end; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent += iterations[4].interval) {
			    for (jobOptimizer.reduceParameters.pMergeThresholdPercent = iterations[5].start; jobOptimizer.reduceParameters.pMergeThresholdPercent <= iterations[5].end; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent += iterations[5].interval) {
				for (jobOptimizer.reduceParameters.pCopyThread = iterations[6].start; jobOptimizer.reduceParameters.pCopyThread <= iterations[6].end; jobOptimizer.reduceParameters.pCopyThread += iterations[6].interval) {
				    for (jobOptimizer.reduceParameters.pNumReduce = iterations[7].start; jobOptimizer.reduceParameters.pNumReduce <= iterations[7].end; jobOptimizer.reduceParameters.pNumReduce += iterations[7].interval) {
					
					System.out.println("the number of iteration is " + iterationNums);
					
					List<Statistics> statisActualList = new ArrayList<Statistics>();
					for (ArrayList<Statistics> hostStatis : hostsStatis) {
					    Statistics statisActual = statisticsPredictor
						    .statisticsPredict(
							    hostStatis,
							    jobOptimizer.mapParameters,
							    jobOptimizer.reduceParameters);
					    
					    printStatis(statisActual);
					    statisActualList.add(statisActual);
					}
					System.out.println("--------------------------------");
					double curJobCost = performanceModel
						.jobCost(
							statisActualList,
							jobOptimizer.mapParameters,
							jobOptimizer.reduceParameters,
							inputSize, clusterConf);
					System.out.println("current job cost is " + curJobCost);
					if (minJobCost == 0
						|| minJobCost > curJobCost) {
					    minJobCost = curJobCost;
					    mapParametersOpt.pSplitSize = jobOptimizer.mapParameters.pSplitSize;
					    mapParametersOpt.pSpillPerc = jobOptimizer.mapParameters.pSpillPerc;
					    mapParametersOpt.pSortFactor = jobOptimizer.mapParameters.pSortFactor;
					    mapParametersOpt.pNumSpillForComb = jobOptimizer.mapParameters.pNumSpillForComb;
					    reduceParametersOpt.pSortFactor = jobOptimizer.reduceParameters.pSortFactor;
					    reduceParametersOpt.pSingleShuffleMemoryLimitPercent = jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent;
					    reduceParametersOpt.pMergeThresholdPercent = jobOptimizer.reduceParameters.pMergeThresholdPercent;
					    reduceParametersOpt.pCopyThread = jobOptimizer.reduceParameters.pCopyThread;
					    reduceParametersOpt.pNumReduce = jobOptimizer.reduceParameters.pNumReduce;
					}
				    }
				}
			    }
			}
		    }
		}
	    }
	}

	// print the optimized parameters
	printOpt(mapParametersOpt, reduceParametersOpt);
    }

}

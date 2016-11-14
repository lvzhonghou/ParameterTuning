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

	// statisArrs represents all the statistics of various files, one
	// element of the statisArrs is one job's statistics
	List<ArrayList<Statistics>> statisArrs = new ArrayList<ArrayList<Statistics>>();
	try {
	    for (String path : paths) {
		ObjectInputStream is = new ObjectInputStream(
			new FileInputStream(path));
		ArrayList<Statistics> statistics = (ArrayList<Statistics>) is
			.readObject();
		statisArrs.add(statistics);
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

	// reset the statisArrs, all the statisArrs of the same hostName are set
	// in the same list
	List<ArrayList<Statistics>> statisList = new ArrayList<ArrayList<Statistics>>();
	for (Statistics statis : statisArrs.get(0)) {
	    ArrayList<Statistics> statisListOneHost = new ArrayList<Statistics>();
	    statisListOneHost.add(statis);
	    statisList.add(statisListOneHost);
	}
	for (int i = 1; i < statisArrs.size(); i++) {
	    // traverse the statisArrs
	    for (Statistics statis : statisArrs.get(i)) {
		String hostName = statis.hostName;
		for (ArrayList<Statistics> statisListOneHost : statisList) {
		    if (hostName.equals(statisListOneHost.get(0).hostName)) {
			statisListOneHost.add(statis);
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
	for (jobOptimizer.mapParameters.pSplitSize = iterations[0].start; jobOptimizer.mapParameters.pSplitSize <= iterations[0].end; jobOptimizer.mapParameters.pSplitSize += iterations[0].interval) {
	    for (jobOptimizer.mapParameters.pSpillPerc = iterations[1].start; jobOptimizer.mapParameters.pSpillPerc <= iterations[1].end; jobOptimizer.mapParameters.pSpillPerc += iterations[1].interval) {
		for (jobOptimizer.mapParameters.pSortFactor = iterations[2].start; jobOptimizer.mapParameters.pSortFactor <= iterations[2].end; jobOptimizer.mapParameters.pSortFactor += iterations[2].interval) {
		    jobOptimizer.reduceParameters.pSortFactor = jobOptimizer.mapParameters.pSortFactor;
		    for (jobOptimizer.mapParameters.pNumSpillForComb = iterations[3].start; jobOptimizer.mapParameters.pNumSpillForComb <= iterations[3].end; jobOptimizer.mapParameters.pNumSpillForComb += iterations[3].interval) {
			for (jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent = iterations[4].start; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent <= iterations[4].end; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent += iterations[4].interval) {
			    for (jobOptimizer.reduceParameters.pMergeThresholdPercent = iterations[5].start; jobOptimizer.reduceParameters.pMergeThresholdPercent <= iterations[5].end; jobOptimizer.reduceParameters.pSingleShuffleMemoryLimitPercent += iterations[5].interval) {
				for (jobOptimizer.reduceParameters.pCopyThread = iterations[6].start; jobOptimizer.reduceParameters.pCopyThread <= iterations[6].end; jobOptimizer.reduceParameters.pCopyThread += iterations[6].interval) {
				    for (jobOptimizer.reduceParameters.pNumReduce = iterations[7].start; jobOptimizer.reduceParameters.pNumReduce <= iterations[7].end; jobOptimizer.reduceParameters.pNumReduce += iterations[7].interval) {
					List<Statistics> statisActualList = new ArrayList<Statistics>();
					for (ArrayList<Statistics> statisListOneHost : statisList) {
					    Statistics statisActual = statisticsPredictor
						    .statisticsPredict(
							    statisListOneHost,
							    jobOptimizer.mapParameters,
							    jobOptimizer.reduceParameters);
					    statisActualList.add(statisActual);
					}
					double curJobCost = performanceModel
						.jobCost(
							statisActualList,
							jobOptimizer.mapParameters,
							jobOptimizer.reduceParameters,
							inputSize, clusterConf);
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
	
	// print the mapParamtersOpt
	System.out.println("the optimized parameters are shown as follows: ");
	System.out.println("mapTask's pSplitSize is " + mapParametersOpt.pSplitSize);
	System.out.println("mapTask's pSpillPerc is " + mapParametersOpt.pSpillPerc);
	System.out.println("mapTask's pSortFactor is " + mapParametersOpt.pSortFactor);
	System.out.println("mapTask's pNumSpillForComb is " + mapParametersOpt.pNumSpillForComb);
	System.out.println("reduceTask's pSortFactor is " + reduceParametersOpt.pSortFactor);
	System.out.println("reduceTask's pSingleShuffleMemoryLimitPercent is " + reduceParametersOpt.pSingleShuffleMemoryLimitPercent);
	System.out.println("reduceTask's pMergeThresholdPercent is " + reduceParametersOpt.pMergeThresholdPercent);
	System.out.println("reduceTask's pCopyThread is " + reduceParametersOpt.pCopyThread);
	System.out.println("reduceTask's pNumReduce is " + reduceParametersOpt.pNumReduce);
    }

}

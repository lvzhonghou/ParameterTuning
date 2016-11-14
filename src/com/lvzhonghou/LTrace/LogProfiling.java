package com.lvzhonghou.LTrace;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.lvzhonghou.common.DiskMergeInShuffle;
import com.lvzhonghou.common.FinalMerge;
import com.lvzhonghou.common.MapTraceData;
import com.lvzhonghou.common.MemMergeInShuffle;
import com.lvzhonghou.common.MergeTrace;
import com.lvzhonghou.common.ReduceTraceData;
import com.lvzhonghou.common.ShuffleInfo;
import com.lvzhonghou.common.ShuffleType;
import com.lvzhonghou.common.SpillTrace;

/**
 * @Description
 * @author zhonghou.lzh
 */
public class LogProfiling {
    static Date mapTaskStart;
    static Date mapStartRun;
    static MapTraceData trace;
    static Date mapSleepStart;
    static Date mapSleepEnd;
    static Date mapTaskEnd;
    static Date spillStart;
    static boolean isMerge = false;
    static boolean isLastMerge = false;
    static ReduceTraceData reduceTrace;
    static Date reduceTaskStart;
    static Date reduceStartRun;
    static Date lastMergeStart;
    static Date lastMergeEnd;
    static int startSpillNumKV;

    /**
     * @Description
     * @param fileName
     */
    public static void reduceProfile(String fileName) {
	File file = new File(fileName);

	if (!file.exists()) {
	    System.out.println("no exist");
	}

	BufferedReader reader = null;
	reduceTrace = new ReduceTraceData();
	reduceTrace.shuffles = new ArrayList<ShuffleInfo>();
	reduceTrace.memMergesInShu = new ArrayList<MemMergeInShuffle>();
	reduceTrace.diskMergesInShu = new ArrayList<DiskMergeInShuffle>();
	reduceTrace.finalMerge = new FinalMerge();
	try {
	    InputStreamReader isr = new InputStreamReader(new FileInputStream(
		    file));
	    reader = new BufferedReader(isr);
	    String tempString = null;
	    SimpleDateFormat sdf = new SimpleDateFormat(
		    "yyyy-MM-dd HH:mm:ss,SSS");
	    while ((tempString = reader.readLine()) != null) {
		String[] logArr = tempString.split(" ");
		if (isContain(logArr, "loaded")
			&& isContain(logArr, "properties")) {
		    reduceTaskStart = sdf.parse(logArr[0] + " " + logArr[1]);
		    continue;
		}
		if (isContain(logArr, "reduce") && isContain(logArr, "task")
			&& isContain(logArr, "start")) {
		    reduceStartRun = sdf.parse(logArr[0] + " " + logArr[1]);
		    reduceTrace.initialCost = reduceStartRun.getTime()
			    - reduceTaskStart.getTime();
		    continue;
		}
		if (isContain(logArr, "memoryLimit")
			&& isContain(logArr, "maxSingleShuffleLimit")) {
		    int index = getIndex(logArr, "memoryLimit") + 2;
		    reduceTrace.memoryLimit = Integer.parseInt(logArr[index]);
		    int index1 = getIndex(logArr, "maxSingleShuffleLimit") + 2;
		    reduceTrace.maxSingleShuffleLimit = Integer
			    .parseInt(logArr[index1]);
		    int index2 = getIndex(logArr, "mergeThreshold") + 2;
		    reduceTrace.mergeThreshold = Integer
			    .parseInt(logArr[index2]);
		    int index3 = getIndex(logArr, "ioSortFactor") + 2;
		    reduceTrace.ioSortFactor = Integer.parseInt(logArr[index3]);
		    int index4 = getIndex(logArr, "maxInMemCopyUse") + 2;
		    reduceTrace.maxInMemCopyUse = Double
			    .parseDouble(logArr[index4]);
		    int index5 = getIndex(logArr,
			    "singleShuffleMemoryLimitPercent") + 2;
		    reduceTrace.singleShuffleMemoryLimitPercent = Double
			    .parseDouble(logArr[index5]);

		    continue;
		}
		if (isContain(logArr, "Fetcher") && isContain(logArr, "fetch")
			&& isContain(logArr, "from")) {
		    ShuffleInfo shuffle = new ShuffleInfo();

		    int index1 = getIndex(logArr, "INFO") + 1;
		    shuffle.fetchThread = logArr[index1].substring(1,
			    logArr[index1].length() - 1);

		    int index2 = getIndex(logArr, "from") + 1;
		    shuffle.hostId = logArr[index2];

		    int index3 = getIndex(logArr, "for:") + 1;
		    shuffle.mapTaskId = logArr[index3].substring(1,
			    logArr[index3].length() - 1);

		    reduceTrace.shuffles.add(shuffle);
		    continue;
		}
		if (isContain(logArr, "shuffle")
			&& isContain(logArr, "decomp:")
			&& isContain(logArr, "MEMORY")) {
		    int index1 = getIndex(logArr, "INFO") + 1;
		    String fetchThread = logArr[index1].substring(1,
			    logArr[index1].length() - 1);
		    ShuffleInfo shuffle = reduceTrace.shuffles
			    .get(reduceTrace.shuffles.size() - 1);
		    for (int i = reduceTrace.shuffles.size() - 1; i >= 0; i--) {
			shuffle = reduceTrace.shuffles.get(i);
			if (shuffle.fetchThread.equals(fetchThread)) {
			    break;
			}
		    }

		    int index2 = getIndex(logArr, "decomp:") + 1;
		    int decomp = Integer.parseInt(logArr[index2]);
		    shuffle.decomLength = decomp;

		    int index3 = getIndex(logArr, "len:") + 1;
		    int comLen = Integer.parseInt(logArr[index3]);
		    shuffle.compressLength = comLen;

		    shuffle.type = ShuffleType.inMemory;

		    continue;
		}
		if (isContain(logArr, "shuffle")
			&& isContain(logArr, "decomp:")
			&& isContain(logArr, "DISK")) {
		    int index1 = getIndex(logArr, "INFO") + 1;
		    String fetchThread = logArr[index1].substring(1,
			    logArr[index1].length() - 1);
		    ShuffleInfo shuffle = reduceTrace.shuffles
			    .get(reduceTrace.shuffles.size() - 1);
		    for (int i = reduceTrace.shuffles.size() - 1; i >= 0; i--) {
			shuffle = reduceTrace.shuffles.get(i);
			if (shuffle.fetchThread.equals(fetchThread)) {
			    break;
			}
		    }

		    int index2 = getIndex(logArr, "decomp:") + 1;
		    int decomp = Integer.parseInt(logArr[index2]);
		    shuffle.decomLength = decomp;

		    int index3 = getIndex(logArr, "len:") + 1;
		    int comLen = Integer.parseInt(logArr[index3]);
		    shuffle.compressLength = comLen;

		    shuffle.type = ShuffleType.onDisk;
		    continue;
		}

		if (isContain(logArr, "start") && isContain(logArr, "end")
			&& isContain(logArr, "shuffle")) {
		    int index1 = getIndex(logArr, "start") + 7;
		    long startShuffle = Long.parseLong(logArr[index1]);

		    int index2 = getIndex(logArr, "end") + 7;
		    long endShuffle = Long.parseLong(logArr[index2]);

		    /*
		     * int index3 = getIndex(logArr, "cost") + 4; long
		     * shuffleCost = Long.parseLong(logArr[index3]);
		     */

		    int index4 = getIndex(logArr, "INFO") + 1;
		    String fetchThread = logArr[index4].substring(1,
			    logArr[index4].length() - 1);
		    ShuffleInfo shuffle = reduceTrace.shuffles
			    .get(reduceTrace.shuffles.size() - 1);
		    for (int i = reduceTrace.shuffles.size() - 1; i >= 0; i--) {
			shuffle = reduceTrace.shuffles.get(i);
			if (shuffle.fetchThread.equals(fetchThread)) {
			    break;
			}
		    }

		    shuffle.startShuffle = startShuffle;
		    shuffle.endShuffle = endShuffle;
		    shuffle.shuffleCost = endShuffle - startShuffle;

		    continue;
		}
		if (isContain(logArr, "[InMemoryMerger")
			&& isContain(logArr, "merge,")
			&& isContain(logArr, "read")
			&& isContain(logArr, "write")) {
		    MemMergeInShuffle memMerge = new MemMergeInShuffle();
		    if (isContain(logArr, "no")) {
			memMerge.haveCombine = false;
			int index1 = getIndex(logArr, "read") + 3;
			memMerge.mergeCost = Long.parseLong(logArr[index1]);
			int index2 = getIndex(logArr, "write") + 3;
			memMerge.writeCost = Long.parseLong(logArr[index2]);
			int index3 = getIndex(logArr,
				"decompressedBytesWritten") + 2;
			memMerge.mergeSize = Integer.parseInt(logArr[index3]);
		    } else {
			memMerge.haveCombine = true;
		    }
		    System.out.println("the mergeCost is " + memMerge.mergeCost
			    + " the writeCost is " + memMerge.writeCost
			    + " mergeSize is " + memMerge.mergeSize);
		    reduceTrace.memMergesInShu.add(memMerge);
		    continue;
		}

		if (isContain(logArr, "[OnDiskMerger")
			&& isContain(logArr, "merge,")
			&& isContain(logArr, "read")
			&& isContain(logArr, "write")) {
		    DiskMergeInShuffle diskMerge = new DiskMergeInShuffle();
		    if (isContain(logArr, "no")) {
			diskMerge.haveCombine = false;
			int index1 = getIndex(logArr, "read") + 3;
			diskMerge.mergeCost = Long.parseLong(logArr[index1]);
			int index2 = getIndex(logArr, "write") + 3;
			diskMerge.writeCost = Long.parseLong(logArr[index2]);
			int index3 = getIndex(logArr,
				"decompressedBytesWritten") + 2;
			diskMerge.mergeSize = Integer.parseInt(logArr[index3]);
		    } else {
			diskMerge.haveCombine = true;
		    }
		    System.out.println("the mergeCost is "
			    + diskMerge.mergeCost + " the writeCost is "
			    + diskMerge.writeCost + " mergeSize is "
			    + diskMerge.mergeSize);
		    reduceTrace.diskMergesInShu.add(diskMerge);
		    continue;
		}

		if (isContain(logArr, "finalMerge")
			&& isContain(logArr, "called")) {
		    reduceTrace.finalMerge.isFinalMerge = true;

		    int index1 = getIndex(logArr, "in-memory") - 1;
		    int numMemMapOutputs = Integer.parseInt(logArr[index1]);
		    reduceTrace.finalMerge.numMemMapOutputs = numMemMapOutputs;

		    int index2 = getIndex(logArr, "on-disk") - 1;
		    int numDiskMapOutputs = Integer.parseInt(logArr[index2]);
		    reduceTrace.finalMerge.numDiskMapOutputs = numDiskMapOutputs;

		    if (reduceTrace.finalMerge.numMemMapOutputs == 0)
			reduceTrace.finalMerge.isCleanMemToDisk = false;

		    if (reduceTrace.ioSortFactor > 0
			    && reduceTrace.finalMerge.numDiskMapOutputs >= reduceTrace.ioSortFactor)
			reduceTrace.finalMerge.isCleanMemToDisk = false;

		    continue;
		}
		if (isContain(logArr, "reduceInBufPerc")
			&& isContain(logArr, "maxInMemReduce")) {
		    int index1 = getIndex(logArr, "reduceInBufPerc") + 2;
		    double reduceInBufPerc = Double.parseDouble(logArr[index1]);
		    reduceTrace.reduceInBufPerc = reduceInBufPerc;

		    int index2 = getIndex(logArr, "maxInMemReduce") + 2;
		    int maxInMemReduce = Integer.parseInt(logArr[index2]);
		    reduceTrace.maxInMemReduce = maxInMemReduce;

		    continue;
		}

		// 当存在numDiskMapOutputs >=
		// pSortFactor时，可能会有中间合并，如果没有中间合并，那么memMapOutputs会在最后一个阶段合并
		if (isContain(logArr, "Keeping") && isContain(logArr, "memory")
			&& reduceTrace.finalMerge.isFinalMerge) {
		    reduceTrace.finalMerge.isCleanMemToDisk = false; // don't
								     // need to
								     // clean
								     // the
								     // memory
								     // into the
								     // disk
		    int index1 = getIndex(logArr, "bytes") - 1;
		    reduceTrace.finalMerge.memToDiskSize = Long
			    .parseLong(logArr[index1]);

		    if (reduceTrace.finalMerge.numDiskMapOutputs == reduceTrace.ioSortFactor)
			reduceTrace.finalMerge.finalMemSize = reduceTrace.finalMerge.memToDiskSize;
		    else
			reduceTrace.finalMerge.finalMemSize = 0;

		    continue;
		}

		// 在FinalMerge时，需要将内存中数据clean到磁盘中
		if (isContain(logArr, "merge,") && isContain(logArr, "read")
			&& isContain(logArr, "write")
			&& reduceTrace.finalMerge.isFinalMerge
			&& reduceTrace.finalMerge.isCleanMemToDisk
			&& isContain(logArr, "decompressedBytesWritten")) {
		    // 首先需要释放内存的数据，合并写入磁盘中(如果需要)
		    if (isContain(logArr, "no"))
			reduceTrace.finalMerge.haveCombiner = false;
		    reduceTrace.finalMerge.isCleanMemToDisk = false;

		    int index1 = getIndex(logArr, "read") + 3;
		    long mergeCost = Long.parseLong(logArr[index1]);
		    reduceTrace.finalMerge.memToDiskMergeCost = mergeCost;

		    int index2 = getIndex(logArr, "write") + 3;
		    long writeCost = Long.parseLong(logArr[index2]);
		    reduceTrace.finalMerge.memToDiskWriteCost = writeCost;

		    int index3 = getIndex(logArr, "decompressedBytesWritten") + 2;
		    long memToDiskFileSize = Long.parseLong(logArr[index3]);
		    reduceTrace.finalMerge.memToDiskSize = memToDiskFileSize;

		    continue;
		}

		// 中间合并，第一次中间合并时，会同时合并内存中和磁盘中部分数据
		if (isContain(logArr, "merge,") && isContain(logArr, "read")
			&& isContain(logArr, "write")
			&& reduceTrace.finalMerge.isFinalMerge
			&& isContain(logArr, "decompressedBytesWritten")
			&& !reduceTrace.finalMerge.isCleanMemToDisk
			&& reduceTrace.finalMerge.isFirstInmeMerge) {
		    reduceTrace.finalMerge.isFirstInmeMerge = false;
		    int index1 = getIndex(logArr, "read") + 3;
		    reduceTrace.finalMerge.immeFirstImmeMergeCost = Long
			    .parseLong(logArr[index1]);
		    int index2 = getIndex(logArr, "write") + 3;
		    reduceTrace.finalMerge.immeFirstImmeWriteCost = Long
			    .parseLong(logArr[index2]);
		    int index3 = getIndex(logArr, "decompressedBytesWritten") + 2;
		    reduceTrace.finalMerge.immeFirstImmeSize = Long
			    .parseLong(logArr[index3]);

		    continue;
		}

		// 非第一次合并,仅合并磁盘中的文件
		if (isContain(logArr, "merge,") && isContain(logArr, "read")
			&& isContain(logArr, "write")
			&& reduceTrace.finalMerge.isFinalMerge
			&& isContain(logArr, "decompressedBytesWritten")
			&& !reduceTrace.finalMerge.isCleanMemToDisk
			&& !reduceTrace.finalMerge.isFirstInmeMerge) {
		    int index1 = getIndex(logArr, "read") + 3;
		    reduceTrace.finalMerge.immeNotFirstImmeMergeCost += Long
			    .parseLong(logArr[index1]);
		    int index2 = getIndex(logArr, "write") + 3;
		    reduceTrace.finalMerge.immeNotFirstImmeWriteCost += Long
			    .parseLong(logArr[index2]);
		    int index3 = getIndex(logArr, "decompressedBytesWritten") + 2;
		    reduceTrace.finalMerge.immeNotFirstImmeSize += Long
			    .parseLong(logArr[index3]);
		    continue;
		}

		// 获取finalMerge的所有数据大小总和
		if (isContain(logArr, "Down") && isContain(logArr, "last")
			&& isContain(logArr, "total")
			&& reduceTrace.finalMerge.isFinalMerge
			&& !reduceTrace.finalMerge.isCleanMemToDisk) {
		    int index1 = getIndex(logArr, "bytes") - 1;
		    reduceTrace.finalMerge.finalMergeSize = Long
			    .parseLong(logArr[index1]);

		    continue;
		}

		/*
		 * if (isContain(logArr, "disk") && isContain(logArr, "satisfy")
		 * && isContain(logArr, "memory") &&
		 * reduceTrace.finalMerge.isFinalMerge) { int index =
		 * getIndex(logArr, "bytes") - 1; int memToDiskSize =
		 * Integer.parseInt(logArr[index]);
		 * reduceTrace.finalMerge.memToDiskSize = memToDiskSize;
		 * 
		 * // System.out.println("memToDiskSize is " // +
		 * reduceTrace.finalMerge.memToDiskSize); continue; }
		 */
		// 在获取最后执行reduce，需要merge的所有大小总和
		/*
		 * if (isContain(logArr, "Merging") && isContain(logArr,
		 * "files,") && isContain(logArr, "disk") &&
		 * reduceTrace.finalMerge.isFinalMerge) { int index =
		 * getIndex(logArr, "bytes") - 1; int finalDiskSize =
		 * Integer.parseInt(logArr[index]);
		 * reduceTrace.finalMerge.finalDiskSize = finalDiskSize; //
		 * System.out.println("finalDiskSize is " // / +
		 * reduceTrace.finalMerge.finalDiskSize);
		 * 
		 * continue; }
		 */
		/*
		 * if (isContain(logArr, "Merging") && isContain(logArr,
		 * "memory") && isContain(logArr, "reduce") &&
		 * reduceTrace.finalMerge.isFinalMerge) { int index =
		 * getIndex(logArr, "bytes") - 1; int finalMemSize =
		 * Integer.parseInt(logArr[index]);
		 * reduceTrace.finalMerge.finalMemSize = finalMemSize;
		 * 
		 * 
		 * continue; }
		 */
		if (isContain(logArr, "read") && isContain(logArr, "write")
			&& isContain(logArr, "reduceOutRecs")
			&& reduceTrace.finalMerge.isFinalMerge) {
		    int index1 = getIndex(logArr, "read") + 3;
		    long finalMergeCost = Long.parseLong(logArr[index1]);
		    reduceTrace.finalMerge.finalMergeCost = finalMergeCost;

		    int index2 = getIndex(logArr, "write") + 3;
		    long hdfsWriteCost = Long.parseLong(logArr[index2]);
		    reduceTrace.hdfsWriteCost = hdfsWriteCost;

		    int index3 = getIndex(logArr, "reduceOutRecs") + 2;

		    int index4 = getIndex(logArr, "num") + 2;
		    long numsReduce = Long.parseLong(logArr[index4]);
		    reduceTrace.numsReduce = numsReduce;

		    System.out.println("numsReduce is "
			    + reduceTrace.numsReduce);

		    // System.out.println("finalMergeCost is " +
		    // reduceTrace.finalMerge.finalMergeCost +
		    // " hdfsWriteCost is " + reduceTrace.hdfsWriteCost );

		    continue;
		}
		if (isContain(logArr, "reduceOutSize")
			&& reduceTrace.finalMerge.isFinalMerge) {
		    int index = getIndex(logArr, "reduceOutSize") + 2;
		    long reduceOutSize = Long.parseLong(logArr[index]);
		    reduceTrace.reduceOutSize = reduceOutSize;

		    continue;
		}
		if (isContain(logArr, "cost") && isContain(logArr, "reduce")
			&& reduceTrace.finalMerge.isFinalMerge) {
		    int index = getIndex(logArr, "reduce");
		    long reduceCost = Long.parseLong(logArr[index + 2]);
		    reduceTrace.reduceCost = reduceCost
			    - reduceTrace.finalMerge.finalMergeCost
			    - reduceTrace.hdfsWriteCost;

		    continue;

		}

	    }

	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ParseException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

    /**
     * @Description
     * @param fileName
     */
    public static void mapProfile(String fileName) {
	File file = new File(fileName);
	BufferedReader reader = null;
	trace = new MapTraceData();
	isMerge = false;
	try {
	    InputStreamReader isr = new InputStreamReader(new FileInputStream(
		    file));
	    reader = new BufferedReader(isr);
	    String tempString = null;
	    SimpleDateFormat sdf = new SimpleDateFormat(
		    "yyyy-MM-dd HH:mm:ss,SSS");
	    while ((tempString = reader.readLine()) != null) {
		String[] logArr = tempString.split(" ");
		if (isContain(logArr, "loaded")
			&& isContain(logArr, "properties")) {
		    mapTaskStart = sdf.parse(logArr[0] + " " + logArr[1]);  // Date
		    continue;
		}
		if (isContain(logArr, "length") && isContain(logArr, "split:")) {
		    int index = getIndex(logArr, "split:") + 1;
		    String splitStr = logArr[index];
		    trace.inputFileSize = Integer.parseInt(splitStr);  // byte
		    continue;
		}
		if (isContain(logArr, "length") && isContain(logArr, "split")) {
		    int index = getIndex(logArr, "split") + 2;
		    String splitStr = logArr[index];
		    trace.inputFileSize = Integer.parseInt(splitStr);  // byte
		    continue;
		}
		if (isContain(logArr, "mapreduce.task.io.sort.mb")) {
		    int index = getIndex(logArr, "is") + 1;
		    String sortMB = logArr[index];
		    trace.sortMB = Integer.parseInt(sortMB);   // MB
		    continue;
		}
		if (isContain(logArr, "soft") && isContain(logArr, "limit")) {
		    int index = getIndex(logArr, "is") + 1;
		    String softLimit = logArr[index];
		    trace.sortLimit = Integer.parseInt(softLimit);  // byte
		    continue;
		}
		if (isContain(logArr, "map") && isContain(logArr, "start")) {
		    mapStartRun = sdf.parse(logArr[0] + " " + logArr[1]);
		    trace.initialCost = mapStartRun.getTime()
			    - mapTaskStart.getTime();     // ms
		    continue;
		}
		/*
		 * if (isContain(logArr, "main") && isContain(logArr, "stop") &&
		 * isContain(logArr, "waiting")) { mapStartRun =
		 * sdf.parse(logArr[0] + " " + logArr[1]); continue; }
		 */

		if (isContain(logArr, "cost") && isContain(logArr, "setup")) {
		    int index = getIndex(logArr, "is") + 1;
		    String setUpStr = logArr[index];
		    trace.setUpCost = Integer.parseInt(setUpStr);    // ns
		    continue;
		}
		if (isContain(logArr, "Spilling") && isContain(logArr, "map")
			&& isContain(logArr, "output")) {
		    trace.spillList.add(new SpillTrace());
		    spillStart = sdf.parse(logArr[0] + " " + logArr[1]);
		    trace.spillList.get(trace.spillList.size() - 1).mapToSpill = spillStart
			    .getTime() - mapStartRun.getTime();      // ms
		    continue;
		}
		if (isContain(logArr, "start") && isContain(logArr, "spill,")
			&& isContain(logArr, "kv")) {
		    int index = getIndex(logArr, "kv") + 2;
		    startSpillNumKV = Integer.parseInt(logArr[index]);  // 个

		    continue;
		}
		if (isContain(logArr, "thread") && isContain(logArr, "start")
			&& isContain(logArr, "wait")) {
		    mapSleepStart = sdf.parse(logArr[0] + " " + logArr[1]);  // date

		    continue;
		}
		if (isContain(logArr, "main") && isContain(logArr, "wait")
			&& isContain(logArr, "kvNums")) {
		    int index = getIndex(logArr, "kvNums") + 2;
		    int mainSleepNumKV = Integer.parseInt(logArr[index]);   

		    trace.mapOutRecsWithSpill += mainSleepNumKV
			    - startSpillNumKV;  // 个
		    trace.mapCostWithSpill += mapSleepStart.getTime()
			    - spillStart.getTime(); // ms   

		    /*
		     * System.out.println("mapOutRecsWithSpill is " +
		     * trace.mapOutRecsWithSpill + " mapCostWithSpill is " +
		     * trace.mapCostWithSpill);
		     */
		    continue;
		}

		if (isContain(logArr, "sort") && isContain(logArr, "mstart")) {
		    int index1 = getIndex(logArr, "start") + 5;
		    long start = Long.parseLong(logArr[index1]);
		    int index2 = getIndex(logArr, "end") + 5;  
		    long end = Long.parseLong(logArr[index2]);
		    trace.spillList.get(trace.spillList.size() - 1).sortTime = end
			    - start;   // ns
		    int index3 = getIndex(logArr, "mstart") + 2;
		    int mstart = Integer.parseInt(logArr[index3]);
		    int index4 = getIndex(logArr, "mend") + 2;
		    int mend = Integer.parseInt(logArr[index4]);
		    trace.spillList.get(trace.spillList.size() - 1).kvNums = Math
			    .abs(mend - mstart);   // 个
		    continue;
		}
		// have combine
		if (isContain(logArr, "read") && isContain(logArr, "write")
			&& isContain(logArr, "reduceOutRecs") && !isMerge) {
		    int index1 = getIndex(logArr, "read") + 3;
		    long readTime = Long.parseLong(logArr[index1]); 
		    trace.spillList.get(trace.spillList.size() - 1).readTime += readTime;  // ns
		    int index2 = getIndex(logArr, "write") + 3;
		    long writeTime = Long.parseLong(logArr[index2]);
		    trace.spillList.get(trace.spillList.size() - 1).writeTime += writeTime;  // ns
		    int index3 = getIndex(logArr, "reduceOutRecs") + 2;
		    int reduceOutRecs = Integer.parseInt(logArr[index3]);
		    trace.spillList.get(trace.spillList.size() - 1).combineOutRecs += reduceOutRecs;  // 个
		    // System.out.println("readtime is " + readTime +
		    // " and writetime is " + writeTime );
		    continue;
		}
		// have combine
		if (isContain(logArr, "combiner,") && isContain(logArr, "have")
			&& !isMerge) {
		    trace.spillList.get(trace.spillList.size() - 1).haveCombiner = true;

		    int index = getIndex(logArr, "is:") + 1;
		    long combineAndWrite = Long.parseLong(logArr[index]);
		    trace.spillList.get(trace.spillList.size() - 1).combineAndWrite = combineAndWrite;  // ns
		    continue;
		}
		// have no combiner
		if (isContain(logArr, "no") && isContain(logArr, "Combiner,")
			&& isContain(logArr, "write")) {
		    int index = getIndex(logArr, "is:") + 1;
		    long writeTime = Long.parseLong(logArr[index]);
		    trace.spillList.get(trace.spillList.size() - 1).haveCombiner = false;
		    trace.spillList.get(trace.spillList.size() - 1).writeTime = writeTime;  // ns
		    trace.spillList.get(trace.spillList.size() - 1).combineAndWrite = writeTime;   // ns
		    trace.spillList.get(trace.spillList.size() - 1).combineOutRecs = trace.spillList
			    .get(trace.spillList.size() - 1).kvNums;  // 个

		    continue;
		}

		if (isContain(logArr, "start") && isContain(logArr, "spill")
			&& isContain(logArr, "end")
			&& !isContain(logArr, "last")) {
		    int index1 = getIndex(logArr, "start") + 5;   
		    long startSpill = Long.parseLong(logArr[index1]);
		    int index2 = getIndex(logArr, "end") + 5;
		    long endSpill = Long.parseLong(logArr[index2]);
		    trace.spillList.get(trace.spillList.size() - 1).spillTime = endSpill
			    - startSpill;   // ns
		    continue;
		}
		if (isContain(logArr, "thread") && isContain(logArr, "stop")
			&& isContain(logArr, "waiting")) {
		    mapSleepEnd = sdf.parse(logArr[0] + " " + logArr[1]);
		    mapStartRun = sdf.parse(logArr[0] + " " + logArr[1]);
		    trace.spillList.get(trace.spillList.size() - 1).mainThreadSleep = mapSleepEnd
			    .getTime() - mapSleepStart.getTime();  // ms
		    continue;
		}
		if (isContain(logArr, "cost") && isContain(logArr, "read")
			&& isContain(logArr, "map")) {
		    int index = logArr.length - 1;
		    trace.readAndMapCost = Long.parseLong(logArr[index]);   // ns
		    continue;
		}
		if (isContain(logArr, "cost") && isContain(logArr, "map")) {
		    int index = logArr.length - 1;
		    trace.mapCost = Long.parseLong(logArr[index]);   // ns
		    continue;
		}
		if (isContain(logArr, "num") && isContain(logArr, "maps")) {
		    int index = logArr.length - 1;
		    trace.maps = Integer.parseInt(logArr[index]);  // 个
		    continue;
		}
		if (isContain(logArr, "cleanup")) {
		    int index = logArr.length - 1;
		    trace.cleanUp = Integer.parseInt(logArr[index]);  // ns
		    continue;
		}
		if (isContain(logArr, "mapOutBytes:")) {
		    int index = logArr.length - 1;
		    trace.mapOutBytes = Integer.parseInt(logArr[index]);  // byte
		    continue;
		}
		if (isContain(logArr, "mapOutRecs:")) {
		    int index = logArr.length - 1;
		    trace.mapOutRecs = Integer.parseInt(logArr[index]);  // 个
		    continue;
		}
		if (isContain(logArr, "number") && isContain(logArr, "spill")
			&& isContain(logArr, "size:")) {
		    int index = getIndex(logArr, "number") + 1;
		    int number = Integer.parseInt(logArr[index]);
		    int index1 = logArr.length - 1;
		    trace.spillList.get(number).spillOutFile = Integer
			    .parseInt(logArr[index1]);   // byte
		    continue;
		}
		if (isContain(logArr, "sortFactor")
			&& isContain(logArr, "numSpillForComb")) {
		    int index1 = getIndex(logArr, "sortFactor") + 2;
		    int sortFactor = Integer.parseInt(logArr[index1].substring(
			    0, logArr[index1].length() - 1));
		    trace.sortFactor = sortFactor;   // 个  
		    int index2 = logArr.length - 1;  
		    int numSpillForComb = Integer.parseInt(logArr[index2]);
		    trace.numSpillForComb = numSpillForComb;  // 个
		    continue;
		}
		// the start of a new merge
		if (isContain(logArr, "Merging") && isContain(logArr, "sorted")
			&& isContain(logArr, "segments")) {
		    isMerge = true;
		    MergeTrace mergeTrace = new MergeTrace();
		    trace.mergeList.add(mergeTrace);

		    continue;
		}
		if (isContain(logArr, "start")
			&& isContain(logArr, "inmediate")
			&& isContain(logArr, "merge:")) {
		    int index1 = getIndex(logArr, "start") + 5;
		    long start = Long.parseLong(logArr[index1].substring(0,
			    logArr[index1].length() - 3));
		    int index2 = logArr.length - 1;
		    long end = Long.parseLong(logArr[index2]);
		    trace.mergeList.get(trace.mergeList.size() - 1).imediateMergeCost = end
			    - start;   // ns

		    continue;
		}

		if (isContain(logArr, "Down") && isContain(logArr, "last")
			&& isContain(logArr, "merge-pass,") && isMerge) {
		    isLastMerge = true;
		    lastMergeStart = sdf.parse(logArr[0] + " " + logArr[1]);    // date
		    continue;
		}

		// have combiner
		if (isContain(logArr, "read") && isContain(logArr, "write")
			&& isContain(logArr, "reduceOutRecs") && isMerge
			&& isLastMerge) {
		    int index1 = getIndex(logArr, "read") + 3;
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeReadCost = Long
			    .parseLong(logArr[index1]);  // ns
		    int index2 = getIndex(logArr, "write") + 3;
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeWriteCost = Long
			    .parseLong(logArr[index2]);   // ns 
		    int index3 = getIndex(logArr, "reduceOutRecs") + 2;   
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeOutRecs = Long
			    .parseLong(logArr[index3]);   // 个
		    trace.mergeList.get(trace.mergeList.size() - 1).haveCombineWithMerge = true;
		    isLastMerge = false;   // recover the isLastMerge
		    lastMergeEnd = sdf.parse(logArr[0] + " " + logArr[1]);   // date
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeCost = (lastMergeEnd
			    .getTime() - lastMergeStart.getTime()) * 1000000;  // ns = ms * 1000000
		    // the mergeCost consists read cost, write cost and combine cost      

		    continue;
		}

		// have no combiner
		if (isContain(logArr, "no") && isContain(logArr, "read")
			&& isContain(logArr, "write") && isMerge && isLastMerge) {
		    int index1 = getIndex(logArr, "read") + 3;
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeReadCost = Long
			    .parseLong(logArr[index1]);   // ns
		    int index2 = getIndex(logArr, "write") + 3;
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeWriteCost = Long
			    .parseLong(logArr[index2]);    // ns
		    trace.mergeList.get(trace.mergeList.size() - 1).haveCombineWithMerge = false;
		    isLastMerge = false;
		    lastMergeEnd = sdf.parse(logArr[0] + " " + logArr[1]);
		    trace.mergeList.get(trace.mergeList.size() - 1).mergeCost = lastMergeEnd
			    .getTime() - lastMergeStart.getTime();   // ms

		    continue;
		}
		/*
		 * if (isContain(logArr, "merge") && isContain(logArr, "start")
		 * && isContain(logArr, "end")) { int index1 = getIndex(logArr,
		 * "start") + 5; long start =
		 * Long.parseLong(logArr[index1].substring(0,
		 * logArr[index1].length() - 1)); int index2 = logArr.length -
		 * 1; long end = Long.parseLong(logArr[index2]);
		 * trace.mergeList.get(trace.mergeList.size() - 1).mergeCost =
		 * end - start; continue; }
		 * 
		 * if (isContain(logArr, "total") && isContain(logArr, "merge:")
		 * && isContain(logArr, "bytes")) { int index = logArr.length -
		 * 1; trace.mergeList.get(trace.mergeList.size() - 1).mergeSize
		 * = Integer.parseInt(logArr[index]); continue; }
		 */
		if (isContain(logArr, "Task") && isContain(logArr, "done.")) {
		    mapTaskEnd = sdf.parse(logArr[0] + " " + logArr[1]);
		    trace.mapTaskCost = mapTaskEnd.getTime()
			    - mapTaskStart.getTime();   // ms
		    continue;
		}
	    }
	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ParseException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    /**
     * @Description
     * @return
     */
    public static boolean isContain(String[] logArr, String target) {
	for (String str : logArr) {
	    if (str.equals(target))
		return true;
	}
	return false;
    }

    /**
     * @Description
     * @param logArr
     * @param target
     * @return
     */
    public static int getIndex(String[] logArr, String target) {
	for (int i = 0; i < logArr.length; i++) {
	    if (logArr[i].equals(target))
		return i;
	}
	return -1;
    }

    public static void print() {
	System.out.println("initialCost " + trace.initialCost);
	System.out.println("sortMB " + trace.sortMB);
	System.out.println("sortLimit " + trace.sortLimit);
	System.out.println("setUpCost " + trace.setUpCost);
	System.out.println("mapCost " + trace.mapCost);
	System.out.println("maps " + trace.maps);
	System.out.println("cleanUp " + trace.cleanUp);
	System.out.println("inputFileSize " + trace.inputFileSize);
	System.out.println("readAndMapCost " + trace.readAndMapCost);

	System.out.println("sortFactor " + trace.sortFactor);
	System.out.println("numSpillForComb " + trace.numSpillForComb);

	System.out.println("mapTaskCost " + trace.mapTaskCost);
	System.out.println("mapOutBytes " + trace.mapOutBytes);
	System.out.println("mapOutRecs " + trace.mapOutRecs);
	for (int i = 0; i < trace.spillList.size(); i++) {
	    SpillTrace spill = trace.spillList.get(i);
	    System.out.println("spill " + i);
	    System.out.println("mapToSpill " + spill.mapToSpill);
	    System.out.println("mainThreadSleep " + spill.mainThreadSleep);
	    System.out.println("spillTime " + spill.spillTime);
	    System.out.println("sortTime " + spill.sortTime);
	    System.out.println("combineAndWrite " + spill.combineAndWrite);
	    System.out.println("readTime " + spill.readTime);
	    System.out.println("writeTime " + spill.writeTime);
	    System.out.println("spillOutFile " + spill.spillOutFile);
	    System.out.println("kvNums " + spill.kvNums);
	    System.out.println("combineOutRecs " + spill.combineOutRecs);
	}
    }
    /*
     * 
     * public static void main(String[] args) {
     * 
     * if (args.length == 0) {
     * System.out.println("please specify the log file path."); return; }
     * 
     * String fileName = args[0]; profile(fileName); print(); }
     */
}
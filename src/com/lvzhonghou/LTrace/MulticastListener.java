package com.lvzhonghou.LTrace;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月18日 下午10:40:41
 * @version v1。0
 */
public class MulticastListener {

    private int port;
    private String host;
    private static final int splitSize = 128;

    public MulticastListener(String host, int port) {
	this.host = host;
	this.port = port;
    }

    // verify the extracted csScheduleCost
    public void printScheduleCost(Statistics statis) {
	System.out.println("the schedule cost as follows");
	System.out.println("the map task's schedule cost is "
		+ statis.mapStatistics.costStat.csScheduleCost);
	System.out.println("the reduce task's schedule cost is "
		+ statis.reduceStatistics.reduceCostStatistics.csSheduleCost);

    }

    // verify the extracted parameters
    public void printStatis(Statistics statis) {
	System.out.println("the statistics as follows.");
	System.out.println("the hostname is " + statis.hostName);

	System.out.println("the map task's parameters as follows.");
	System.out.println("the pSplitSize is "
		+ statis.mapParameters.pSplitSize);
	System.out.println("the pSortMB is " + statis.mapParameters.pSortMB);
	System.out.println("the pSpillPerc is "
		+ statis.mapParameters.pSpillPerc);
	System.out.println("the pSortFactor is "
		+ statis.mapParameters.pSortFactor);
	System.out.println("the pNumSpillForComb is "
		+ statis.mapParameters.pNumSpillForComb);

	System.out.println("the reduce task's parameters as follows.");
	System.out.println("the pTotalMemorySize is "
		+ statis.reduceParameters.pTotalMemorySize);
	System.out.println("the pMergeThresholdPercent is "
		+ statis.reduceParameters.pMergeThresholdPercent);
	System.out.println("the pSingleShuffleMemoryLimitPercent is "
		+ statis.reduceParameters.pSingleShuffleMemoryLimitPercent);
	System.out.println("the pSortFactor is "
		+ statis.reduceParameters.pSortFactor);
	System.out.println("the pCopyThread is "
		+ statis.reduceParameters.pCopyThread);
    }

    public void listen() {
	byte[] data = new byte[256];
	try {
	    InetAddress ip = InetAddress.getByName(this.host);
	    MulticastSocket ms = new MulticastSocket(this.port);
	    ms.joinGroup(ip);
	    DatagramPacket packet = new DatagramPacket(data, data.length);
	    ms.receive(packet);

	    String jobId = new String(packet.getData(), 0, packet.getLength());
	    ms.close();

	    // extract the statistics
	    Statistics statistics;
	    statistics = extractStatistics(jobId);

	    // send the statistics
	    String hostName = InetAddress.getLocalHost().getHostName();
	    Client client = new Client("192.168.7.102", 2345);

	    if (statistics != null) {
		statistics.hostName = hostName;
	    } else {
		statistics = new Statistics();
		statistics.isEmpty = true;
		statistics.hostName = hostName;
	    }

	    // test the parameters
	    printStatis(statistics);

	    client.send(statistics);

	} catch (UnknownHostException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public Statistics extractStatistics(String jobId) {
	//
	Statistics statistics = null;
	File file = new File("//root//hadoop-2.6.0//logs//userlogs");

	File[] files = file.listFiles();

	for (File ff : files) {
	    String ffName = ff.getName();
	    if (ffName.equals(jobId)) {
		// find the filepath
		File[] containerFiles = ff.listFiles();
		String[] paths = new String[containerFiles.length];
		for (int i = 0; i < containerFiles.length; i++) {
		    File[] subFilesContainer = containerFiles[i].listFiles();
		    for (File subFF : subFilesContainer) {
			if (subFF.getName().equals("syslog")) {
			    paths[i] = subFF.getAbsolutePath();
			    break;
			}
		    }
		}

		statistics = StatisticsExtract.extractStatistics(paths,
			splitSize); // 默认为128MB

		break;
	    }
	}

	//

	return statistics;
    }

    /**
     * @Description
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	int port = 1234;
	String host = "224.0.0.1";
	MulticastListener ml = new MulticastListener(host, port);
	while (true) {
	    ml.listen();
	}
    }

}

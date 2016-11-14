import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.lvzhonghou.LTrace.LogFileClassification;
import com.lvzhonghou.common.MapOrReduce;


/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年6月12日 下午9:48:59
 * @version v1。0
 */
public class LogExecutionCost {
    static int mapInputSize = 128;

    /**
     * @Description
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {
	String jobId = args[0];

	File file = new File("//root//hadoop-2.6.0//logs//userlogs");
	File[] files = file.listFiles();
	String[] paths = null;

	for (File ff : files) {
	    String ffName = ff.getName();
	    if (ffName.equals(jobId)) {
		File[] containerFiles = ff.listFiles();
		paths = new String[containerFiles.length];
		for (int i = 0; i < containerFiles.length; i++) {
		    File[] subFilesContainer = containerFiles[i].listFiles();
		    for (File subFF : subFilesContainer) {
			if (subFF.getName().equals("syslog")) {
			    paths[i] = subFF.getAbsolutePath();
			    break;
			}
		    }
		}
	    }
	}

	if (paths == null || paths.length == 0) {
	    System.out.println("no container files.");
	    return;
	}
	List<String> mapFiles = new ArrayList<String>();
	List<String> reduceFiles = new ArrayList<String>();
	for (String path : paths) {
	    // 该日志文件是否完整，如果是完整日志文件，还需要判断该日志文件是mapFiles还是reduceFiles
	    boolean isComplete = LogFileClassification.isComplete(path);
	    if (isComplete) {
		MapOrReduce mapOrReduce = LogFileClassification
			.fileClassify(path);
		if (mapOrReduce == MapOrReduce.map) {
		    if ((LogFileClassification.getSplitSize(path) * 1.0)
			    / (mapInputSize * 1024 * 1024 * 1.0) > 0.8)
			mapFiles.add(path);
		} else
		    reduceFiles.add(path);
	    }
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	// calcu the average cost of map and reduce tasks
	List<Long> mapCosts = new ArrayList<Long>();
	List<Long> reduceCosts = new ArrayList<Long>();
	if (mapFiles.size() > 0) {
	    System.out.println("There are " + mapFiles.size() + " map files.");
	    for (String path : mapFiles) {
		String firstLine = LogFileClassification.getFirstLine(path);
		String lastLine = LogFileClassification.getLastLine(path);
		Date start, end;
		String[] logArr = firstLine.split(" ");
		start = sdf.parse(logArr[0] + " " + logArr[1]);
		String[] logArr1 = lastLine.split(" ");
		end = sdf.parse(logArr1[0] + " " + logArr1[1]);
		mapCosts.add(end.getTime() - start.getTime());
	    }
	}
	if (reduceFiles.size() > 0) {
	    System.out.println("There are " + reduceFiles.size()+ " reduce files.");
	    for (String path : reduceFiles) {
		String firstLine = LogFileClassification.getFirstLine(path);
		String lastLine = LogFileClassification.getLastLine(path);
		Date start, end;
		String[] logArr = firstLine.split(" ");
		String[] logArr1 = lastLine.split(" ");
		start = sdf.parse(logArr[0] + " " + logArr[1]);
		end = sdf.parse(logArr1[0] + " " + logArr1[1]);
		reduceCosts.add(end.getTime() - start.getTime());
	    }
	}

	double mapCostAvg = 0, reduceCostAvg = 0;
	if (mapCosts.size() > 0) {
	    for (Long mapCost : mapCosts)
		mapCostAvg += mapCost;
	    mapCostAvg = mapCostAvg / (mapCosts.size() * 1.0);
	    System.out.println("the avg map cost is " + mapCostAvg);
	}
	if(reduceCosts.size() > 0) {
	    for(Long reduceCost : reduceCosts) 
		reduceCostAvg += reduceCost;
	    reduceCostAvg = reduceCostAvg / (reduceCosts.size() * 1.0);
	    System.out.println("the avg reduce cost is " + reduceCostAvg);
	}

    }
}

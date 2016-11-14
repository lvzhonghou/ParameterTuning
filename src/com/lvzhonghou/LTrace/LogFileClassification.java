package com.lvzhonghou.LTrace;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.lvzhonghou.common.MapOrReduce;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月19日 下午6:13:34
 * @version v1。0
 */


public class LogFileClassification {
    public static boolean isComplete(String path) {
	// first line
	String firstLine = getFirstLine(path);
	// last line
	String lastLine = getLastLine(path);

	String[] firstLineArr = firstLine.split(" ");
	String[] lastLineArr = lastLine.split(" ");

	boolean isComplete = isContain(firstLineArr, "loaded")
		&& (isContain(lastLineArr, "done.") || isContain(lastLineArr,
			"complete."));
	return isComplete;
    }

    public static MapOrReduce fileClassify(String path) {
	// the third line
	String thirdLine = getThirdLine(path);

	String[] thirdLineArr = thirdLine.split(" ");
	if (isContain(thirdLineArr, "MapTask")) {
	    return MapOrReduce.map;
	} else {
	    return MapOrReduce.reduce;
	}
    }

    private static boolean isContain(String[] strs, String keyStr) {
	for (String str : strs) {
	    if (str.equals(keyStr)) {
		return true;
	    }
	}

	return false;
    }

    public static int getSplitSize(String path) {
	RandomAccessFile raf;
	String line = null;
	int splitSize = 0;

	try {
	    raf = new RandomAccessFile(path, "r");

	    while ((line = raf.readLine()) != null) {
		String[] logArr = line.split(" ");
		if (isContain(logArr, "length")
			&& (isContain(logArr, "split:") || isContain(logArr,
				"split"))) {
		    int index = logArr.length - 1;
		    String str = logArr[index];

		    splitSize = Integer.parseInt(str);
		    break;
		}
	    }
	} catch (NumberFormatException | IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return splitSize;
    }

    public static String getThirdLine(String path) {
	RandomAccessFile raf;
	String thirdLine = null;

	try {
	    raf = new RandomAccessFile(path, "r");
	    for (int i = 0; i < 3; i++) {
		thirdLine = raf.readLine();
	    }
	    raf.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return thirdLine;

    }

    public static String getLastLine(String path) {
	RandomAccessFile raf;
	String lastLine = null;

	try {
	    raf = new RandomAccessFile(path, "r");
	    long len = raf.length();
	    if (len != 0L) {
		long pos = len - 1;
		while (pos > 0) {
		    pos--;
		    raf.seek(pos);
		    if (raf.readByte() == '\n') {
			lastLine = raf.readLine();
			break;
		    }
		}
	    }
	    raf.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return lastLine;
    }

    public static String getFirstLine(String path) {
	RandomAccessFile raf;
	String firstLine = null;

	try {
	    raf = new RandomAccessFile(path, "r");
	    firstLine = raf.readLine();
	    raf.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return firstLine;

    }
}

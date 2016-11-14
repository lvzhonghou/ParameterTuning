package com.lvzhonghou.LTrace;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月19日 下午3:32:49
 * @version v1。0
 */
public class Server {
    private int port;
    private List<Statistics> statistics;
    private int numSlaves;
    private String jobId;

    public Server(int port, int numSlaves, String jobId) {
	this.port = port;
	this.statistics = new ArrayList<Statistics>();
	this.numSlaves = numSlaves;
	this.jobId = jobId;
    }

    /**
     * @Description
     * @param args
     */

    public void listen() {
	try {
	    ServerSocket server = new ServerSocket(port);
//	    server.setSoTimeout(2000);
	    int numConnection = 0;
	    while (true) {
		Socket socket = server.accept();
		
		this.invoke(socket);
		
		numConnection++;
		if (numConnection == this.numSlaves)
		    break;
		
	    }
	    
	    Thread.sleep(10000);
	    
	    //子线程不会受到主线程的生命周期的影响
	    
	} catch (IOException | InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void invoke(final Socket socket) {
	new Thread(new Runnable() {

	    @Override
	    public void run() {
		// TODO Auto-generated method stub
		ObjectInputStream is = null;
		ObjectOutputStream os = null;
		try {
		    is = new ObjectInputStream(new BufferedInputStream(
			    socket.getInputStream()));
		    os = new ObjectOutputStream(socket.getOutputStream());

		    Object obj;

		    obj = is.readObject();

		    Statistics statistics = (Statistics) obj;

		    // 触发存储操作 hand 
		    handle(statistics);
		} catch (IOException | ClassNotFoundException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		} finally {
		    try {
			is.close();
			os.close();
			socket.close();
		    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }

		}
	    }
	}).start();
    }

    public synchronized void handle(Statistics statistics) throws IOException {
	this.statistics.add(statistics);

	// if collection is over, write the object to the file
	if (this.statistics.size() == this.numSlaves) {
	    String filePath = this.jobId + ".txt";

	    File file = new File(filePath);
	    if (!file.exists()) {
		file.createNewFile();
	    }

	    ObjectOutputStream os = new ObjectOutputStream(
		    new FileOutputStream(file));
	    os.writeObject(this.statistics);
	    os.close();
	}
    }

    public List<Statistics> getStatisticsList() {
	return this.statistics;
    }

}

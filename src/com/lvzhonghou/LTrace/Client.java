package com.lvzhonghou.LTrace;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.lvzhonghou.common.Statistics;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月19日 下午2:36:42
 * @version v1。0
 */
public class Client {
    String host;
    int port;
    
    public Client(String host, int port) {
	this.host = host;
	this.port = port;
    }
    
    public void send(Statistics statistics) {
	Socket socket = null;
	ObjectOutputStream os = null;
	ObjectInputStream is = null;

	try {
	    socket = new Socket(host, port);
	    
	    os = new ObjectOutputStream(socket.getOutputStream());
	    os.writeObject(statistics);
	    os.flush();
	    Thread.sleep(3000);
	} catch (IOException | InterruptedException e) {
	    System.out.println("error");
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} finally {
	    try {
		os.close();
		socket.close();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}
    }
}

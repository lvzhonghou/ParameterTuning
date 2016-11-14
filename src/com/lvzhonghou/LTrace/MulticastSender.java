package com.lvzhonghou.LTrace;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

/**
 * @Description
 * @author zhonghou.lzh
 * @date 2016年3月18日 下午10:07:33
 * @version v1。0
 */
public class MulticastSender {
    private int port;
    private String host;
    private String data;
    private final static int numSlaves = 14;

    public MulticastSender(String data, String host, int port) {
	this.data = data;
	this.host = host;
	this.port = port;
    }

    public void send() {
	try {
	    InetAddress ip = InetAddress.getByName(this.host);
	    DatagramPacket packet = new DatagramPacket(this.data.getBytes(),
		    this.data.length(), ip, this.port);
	    MulticastSocket ms = new MulticastSocket();
	    ms.send(packet);
	    ms.close();
	} catch (UnknownHostException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    
    public static void main(String[] args) {
	int port = 1234;
	String host = "224.0.0.1";
	if(args.length == 0)
	    System.out.println("please input the jobId");
	String jobId = args[0];
	
	MulticastSender ms = new MulticastSender(jobId, host, port);
	ms.send();
	
	//打开server
	Server server = new Server(2345, numSlaves, jobId);
	server.listen();
    }
}

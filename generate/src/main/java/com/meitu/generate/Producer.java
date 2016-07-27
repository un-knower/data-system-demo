package com.meitu.generate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantLock;

public class Producer implements Runnable{
	private String directory;
	private int thread_num;
	private int thread_alive_num;
	private ReentrantLock lock = new ReentrantLock();
	private boolean shutdown;
	private QueueConsumer qcon;
	
	synchronized private void threadAliveIncrease(){
		thread_alive_num++;
		System.out.println("alive++:"+thread_alive_num);
	} 
	synchronized private void threadAliveDecrease(){
		thread_alive_num--;
		System.out.println("alive--:"+thread_alive_num);
	}
	public Producer(String directory){
		this(directory,1);
	}
	public Producer(String directory, int thread_num){
		this.directory = directory;
		this.thread_num = thread_num;
		shutdown = false;
		thread_alive_num = 0;
	}
	/**
	 * 停止生产
	 * 阻塞方法，直到所有生产者结束
	 * */
	public void stop(){
		shutdown = true;
		while(thread_alive_num>0){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//Log.info();
		System.out.println("All producers have shut down!");
		qcon.flushToSink();
		System.out.println("All records have sunk to files!");
		qcon.stop();
		System.out.println("Send stop signal to Consumer");
	}
	/**
	 * 开始生产
	 * */
	public void start(){
		if(qcon==null)
			qcon = new QueueConsumer(5000,1000,500,10000,1);
		for(int i=0;i<thread_num;i++)
			new Thread(this).start();
	}
	public void run(){
		Integer listStamp = null;
		try{	
			threadAliveIncrease();
			listStamp = qcon.getBufferList();
			while(!shutdown){
				File file = new File(directory);
				File[] files = file.listFiles();
				Scanner sc = null;
				boolean didNothing = true;
				for(int i=0;!shutdown&&i<files.length;i++){// for each file in directory
					String filename = files[i].getName();
					if(filename.length()>5&&(filename.substring(filename.length()-5, filename.length()).equals("-done")
							||filename.substring(filename.length()-5, filename.length()).equals("-lock")))// ignore files end with "-done" and "-lock"
						continue;
					//竞争改名，理想情况是1.支持不同进程的线程间竞争；2.只有与该文件相关的线程之间存在竞争
					boolean renameFlag = false;
					lock.lock();
					if(files[i].getName().equals(filename)){
						renameFlag = files[i].renameTo(new File(files[i].getPath()+"-lock"));
					}
					lock.unlock();
					if(!renameFlag)
						continue;
					//add to queue
					sc = new Scanner(new FileInputStream(files[i].getPath()+"-lock"));
					while(sc.hasNextLine()){
						String line = sc.nextLine();
						qcon.addElement(listStamp, line);
					}
					sc.close();
					didNothing = false;
					//rename
					new File(files[i].getPath()+"-lock").renameTo(new File(files[i].getPath()+"-done"));
				}
				if(!shutdown&&didNothing){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}catch(Exception e){	
			e.printStackTrace();
		}finally{
			if(listStamp!=null)
				qcon.flushBufferList(listStamp);//flush buffer to toSink
			threadAliveDecrease();//保证线程结束一定alive--
		}
	}
}
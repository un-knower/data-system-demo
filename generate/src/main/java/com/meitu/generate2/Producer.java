package com.meitu.generate2;

import java.io.File;
import java.io.FileInputStream;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantLock;

public class Producer implements Runnable {
    private String directory;
    private int threadNum;
    private int threadAliveNum;
    private ReentrantLock lock = new ReentrantLock();
    private boolean shutdown;
    private ConsumerNoticeConcurrentLinkedQueue<String> queue;

    synchronized private void threadAliveIncrease() {
        threadAliveNum++;
        System.out.println("alive++:" + threadAliveNum);
    }

    synchronized private void threadAliveDecrease() {
        threadAliveNum--;
        System.out.println("alive--:" + threadAliveNum);
    }

    public Producer(String directory, ConsumerNoticeConcurrentLinkedQueue<String> queue) {
        this(directory, 1, queue);
    }

    public Producer(String directory, int threadNum, ConsumerNoticeConcurrentLinkedQueue<String> queue) {
        this.directory = directory;
        this.threadNum = threadNum;
        this.queue = queue;
        shutdown = false;
        threadAliveNum = 0;
    }

    /**
     * 停止生产 阻塞方法，直到所有生产者结束
     */
    public void stop() {
        shutdown = true;
        while (threadAliveNum > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // Log.info();
        System.out.println("All producers have shut down!");
    }

    /**
     * 开始生产
     * 
     * @throws Exception
     */
    public void start() throws Exception {
        if (queue == null)
            throw new Exception("生产队列为空！启动失败！");
        for (int i = 0; i < threadNum; i++)
            new Thread(this).start();
    }

    public void run() {
        try {
            threadAliveIncrease();
            while (!shutdown) {
                File file = new File(directory);
                File[] files = file.listFiles();
                Scanner sc = null;
                boolean didNothing = true;
                for (int i = 0; !shutdown && i < files.length; i++) {// for each
                                                                     // file in
                                                                     // directory
                    String filename = files[i].getName();
                    if (filename.length() > 5
                            && (filename.substring(filename.length() - 5, filename.length()).equals("-done")
                                    || filename.substring(filename.length() - 5, filename.length()).equals("-lock")))// ignore
                                                                                                                     // files
                                                                                                                     // end
                                                                                                                     // with
                                                                                                                     // "-done"
                                                                                                                     // and
                                                                                                                     // "-lock"
                        continue;
                    // 竞争改名，理想情况是1.支持不同进程的线程间竞争；2.只有与该文件相关的线程之间存在竞争
                    boolean renameFlag = false;
                    lock.lock();
                    if (files[i].getName().equals(filename)) {
                        renameFlag = files[i].renameTo(new File(files[i].getPath() + "-lock"));
                    }
                    lock.unlock();
                    if (!renameFlag)
                        continue;
                    // add to queue
                    sc = new Scanner(new FileInputStream(files[i].getPath() + "-lock"));
                    while (sc.hasNextLine()) {
                        String line = sc.nextLine();
                        queue.addFirst(line);
                    }
                    sc.close();
                    didNothing = false;
                    // rename
                    new File(files[i].getPath() + "-lock").renameTo(new File(files[i].getPath() + "-done"));
                }
                if (!shutdown && didNothing) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadAliveDecrease();// 保证线程结束一定alive--
        }
    }
}
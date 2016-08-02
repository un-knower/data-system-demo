package com.meitu.generate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 生产者与消费者在同一个JVM，使用共享内存作队列
 * 
 * buffer：生产者缓冲区队列 toSink：消费者缓冲区 bufferSize：Sink条件，toSink中记录数>bufferSize时Sink
 * timeout：Sink条件，距离上一次Sink时间超过timeout则Sink
 * batchFactor：生产者上报条件，producer产生记录>=batchFactor则上报到toSink；双缓冲区使得buffer.size()+
 * toSink.size()>=bufferSize；batchFactor设为1则==成立
 */
public class QueueConsumer {
    private List<List<String>> buffer;
    private LinkedList<Integer> returnedBufferList;
    private long timeout;
    private int bufferSize;
    private int batchFactor;
    private Thread sinkThread;
    private boolean shutdown;
    private List<List<String>> toSink;
    private ReentrantLock toSinklock;
    private int toSinkSize;

    private int checkNewLogFactor;// 至多checkNewLogFactor这么多写到错误的log中
    private long lastLogStamp;
    private String outputDirectory;
    private long newLogInMinute;

    public void stop() {
        shutdown = true;
    }

    /**
     * @param timeout
     * @param bufferSize
     * @param batchFactor
     * @param checkNewLogFactor
     * @param logInterval
     */
    public QueueConsumer(long timeout, int bufferSize, int batchFactor, int checkNewLogFactor, int newLogInMinute, String outputDirectory) {
        this.outputDirectory = outputDirectory;
        this.timeout = timeout;
        this.bufferSize = bufferSize;
        this.batchFactor = batchFactor;
        this.checkNewLogFactor = checkNewLogFactor;
        this.newLogInMinute = newLogInMinute;
        buffer = new LinkedList<List<String>>();// LinkedList适合增加删除
        shutdown = false;
        sinkThread = new Thread(new SinkToFile());
        sinkThread.start();
        toSink = new LinkedList<List<String>>();
        toSinkSize = 0;
        toSinklock = new ReentrantLock();
        lastLogStamp = 0;
    }

    /**
     * 获取一个BufferList
     * 
     * @param stamp
     *            bufferList的标记
     */
    synchronized public int getBufferList() {
        List<String> list = new ArrayList<String>();
        if (returnedBufferList == null || returnedBufferList.size() == 0) {
            buffer.add(list);
            return buffer.size() - 1;
        } else {
            int stamp = returnedBufferList.pop();
            buffer.set(stamp, list);
            return stamp;
        }
    }

    // 一个线程一个stamp，混用有不可估计的后果
    /**
     * 释放bufferList
     */
    public void returnBufferList(int stamp) {
        buffer.set(stamp, null);
        if (!returnedBufferList.contains(stamp))// 存在查找过程
            returnedBufferList.add(stamp);
    }

    /**
     * 添加，阻塞
     */
    public void addElement(int stamp, String obj) {
        List<String> list = buffer.get(stamp);
        list.add(obj);
        if (list.size() >= batchFactor) {// batchFactro提交到toSink
            flushBufferList(stamp);
        }
    }

    /**
     * flush toSink to file
     */
    public void flushToSink() {
        sinkToFile();
    }

    /**
     * flush lsit buffer[stamp] from buffer to toSink
     */
    public void flushBufferList(int stamp) {
        List<String> list = buffer.get(stamp);
        toSinklock.lock();// 消费太慢此处会block
        toSink.add(list);// 追加，一个生产者可以追加多次
        toSinkSize += list.size();
        if (toSinkSize >= bufferSize)// bufferSize通知消费toSink
            sinkThread.interrupt();
        toSinklock.unlock();
        buffer.set(stamp, new ArrayList<String>());
    }

    private void sinkToFile() {
        // System.out.println("Sink to File, SinkSize:"+toSinkSize);
        // sink to File
        try {
            toSinklock.lock();
            String output = getLogPath();// 保证即使没有消费到record，也能产生空日志文件
            PrintWriter pw = new PrintWriter(new FileOutputStream(output, true));
            int tik = 0;
            for (int i = 0; i < toSink.size(); i++) {
                List<String> list = toSink.get(i);
                if (list == null || list.size() == 0) {
                    continue;
                }
                for (int j = 0; j < list.size(); j++) {
                    if (++tik >= this.checkNewLogFactor) {
                        tik = 0;
                        // check output path
                        if (checkLogPath(output)) {
                            output = getLogPath();
                            if (pw != null)
                                pw.close();
                            pw = new PrintWriter(new FileOutputStream(output, true));
                        }
                    }
                    pw.println(list.get(j));
                }
            }
            toSink = new LinkedList<List<String>>();
            toSinkSize = 0;
            if (pw != null)
                pw.close();
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } finally {
            toSinklock.unlock();
        }
    }

    /**
     * true when there is a new log
     */
    private boolean checkLogPath(String oldPath) {
        Date date = new Date();
        if (date.after(new Date(this.lastLogStamp + this.newLogInMinute * 60 * 1000))) {
            return true;
        }
        return false;
    }

    private String getLogPath() {
        boolean needNewFile = false;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        Date date = new Date();
        if (date.after(new Date(this.lastLogStamp + this.newLogInMinute * 60 * 1000))) {
            lastLogStamp = (date.getTime() / (60 * 1000)) * 60 * 1000;// 时间取整
            needNewFile = true;
        }
        String filePath = outputDirectory + "\\content." + sdf.format(new Date(lastLogStamp)) + ".log";
        if (needNewFile) {
            File file = new File(filePath);
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return filePath;
    }

    private class SinkToFile implements Runnable {
        public void run() {
            // TODO Auto-generated method stub
            while (!shutdown) {
                sinkToFile();
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    // e.printStackTrace();
                }
            }
            System.out.println("Sink Thread shutdown!");
        }

    }
}
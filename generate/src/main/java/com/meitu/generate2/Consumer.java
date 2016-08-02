package com.meitu.generate2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Consumer<E> extends Thread {
    private ConsumerNoticeConcurrentLinkedQueue<E> queue;
    private long timeout;
    private int shutdown;// -1 shutdown now; 1 consume until queue is empty,
                         // then shutdown;
    private long lastLogStamp;
    private int logInterval;// minute
    private String outputDirectory;
    private int checkNewLogFactor;// 日志最大错写数量=checkNewLogFactor-1

    /**
     * 1.timeout时间自动消费queue 2.interrupt启动消费
     * 
     * @param queue
     *            消费队列
     * @param timeout
     *            消费间隔，间隔为0时不断尝试消费，<0时间隔为0
     * @param logInterval
     *            日志划分时间
     * @param logDirectory
     *            日志输出目录
     * @param maxWrongNum
     *            日志中允许的最多时间错误记录数目
     */
    public Consumer(ConsumerNoticeConcurrentLinkedQueue<E> queue, long timeout, int logInterval, String logDirectory,
            int maxWrongNum) {
        this.queue = queue;
        this.timeout = timeout > 0 ? timeout : 0;
        this.shutdown = 2;
        this.lastLogStamp = logInterval;
        this.logInterval = logInterval;
        this.outputDirectory = logDirectory;
        this.checkNewLogFactor = maxWrongNum + 1;
    }

    @Override
    public void start() {
        super.start();
        shutdown = 0;
    }

    public void shutdown() {
        shutdown = 1;
    }

    public void shutdownImmediately() {
        shutdown = -1;
    }

    /**
     * 手动通知消费
     */
    public void consume() {
        this.interrupt();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        while (shutdown != -1) {
            if (queue == null || queue.size() == 0) {
                if (shutdown == 1)
                    shutdown = -1;
            } else {
                // 如果sinkToFile期间中断状态被置位，接下来的sleep方法不会sleep，而是直接抛出InterruptedException。
                // 所以如果sinkToFile期间Consumer再次被通知消费，那么它不会sleep，而是开始下次sinkToFile。
                sinkToFile();
            }
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
            }
        }
        System.out.println("consumer stop success");
    }

    /**
     * true when there is a new log
     */
    private boolean checkLogPath() {
        Date date = new Date();
        if (date.after(new Date(this.lastLogStamp + this.logInterval * 60 * 1000))) {
            return true;
        }
        return false;
    }

    /**
     * Return current log path. Create a new log path when longer than
     * logInterval minutes has passes since last log creation
     */
    private String getLogPath() {
        boolean needNewFile = false;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        Date date = new Date();
        if (date.after(new Date(lastLogStamp + logInterval * 60 * 1000))) {
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

    /**
     * Sink queue to log
     */
    private void sinkToFile() {
        try {
            String output = getLogPath();
            PrintWriter pw = new PrintWriter(new FileOutputStream(output, true));
            int tik = 0;
            int consumeSize = queue.size();
            for (int i = 0; i < consumeSize; i++) {
                if (++tik >= this.checkNewLogFactor) {
                    // System.out.println(checkLogPath()+" "+(new
                    // Date().getTime()-this.lastLogStamp)/1000);
                    tik = 0;
                    // check output path
                    if (checkLogPath()) {
                        output = getLogPath();
                        if (pw != null)
                            pw.close();
                        pw = new PrintWriter(new FileOutputStream(output, true));
                    }
                }
                Object obj = queue.pollLast();
                if (obj == null)// queue is empty, shouldn't happen as there is
                                // only one consumer
                    return;
                pw.println(obj);
            }
            if (pw != null)
                pw.close();
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }
}
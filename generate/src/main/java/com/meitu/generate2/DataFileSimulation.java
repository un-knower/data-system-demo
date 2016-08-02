package com.meitu.generate2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFileSimulation {
    private static Logger logger = LoggerFactory.getLogger(DataFileSimulation.class);
    private ConsumerNoticeConcurrentLinkedQueue queue;
    private int producerNum;
    private int bufferSize;
    private Producer producer;
    private Consumer consumer;
    private final String producerDirectory;

    public DataFileSimulation(int producerNum, int bufferSize, long timeout, int logInterval, String logDirectory,
            int maxWrongNum, String producerDirectory) {
        this.producerNum = producerNum;
        this.bufferSize = bufferSize;
        this.producerDirectory = producerDirectory;
        // create queue
        queue = new ConsumerNoticeConcurrentLinkedQueue<String>();
        // create consumer
        consumer = new Consumer<String>(queue, timeout, logInterval, logDirectory, maxWrongNum);
        // create producer
        producer = new Producer(producerDirectory, producerNum, queue);
        // create (consumer) notice list
        List<Consumer> list = new ArrayList<Consumer>();
        list.add(consumer);
        // set notice list
        queue.setNoticeList(list);
        queue.setNoticeSize(bufferSize);
    }

    public void start() throws Exception {
        producer.start();
        consumer.start();
    }

    public void stop() {
        System.out.println("==start stoping==");
        producer.stop();
        System.out.println("==producer stop success==");
        consumer.consume();// 通知consumer消费，消费完queue中所有内容
        consumer.shutdown();
        System.out.println("==send signal to stop consumer==");
    }

    public static void main(String args[]) throws Exception {

        String path = "D:\\Produce";
        File file = new File(path);
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.getName().length() > 5
                    && f.getName().substring(f.getName().length() - 5, f.getName().length()).equals("-done"))
                f.renameTo(new File(f.getPath().substring(0, f.getPath().length() - 5)));
            if (f.getName().length() > 5
                    && f.getName().substring(f.getName().length() - 5, f.getName().length()).equals("-lock"))
                f.renameTo(new File(f.getPath().substring(0, f.getPath().length() - 5)));
        }

        int producerNum = 3;
        int bufferSize = 1000;
        long timeout = 5000;
        int logInterval = 1;
        String logDirectory = "D:\\Consume";
        int maxWrongNum = 9999;
        String producerDirectory = "D:\\Produce";
        DataFileSimulation dfs = new DataFileSimulation(producerNum, bufferSize, timeout, logInterval, logDirectory,
                maxWrongNum, producerDirectory);
        dfs.start();
        Scanner sc = new Scanner(System.in);
        while (true) {
            String line = sc.nextLine();
            if (line.equals("exit") || line.equals("shutdown") || line.equals("close"))
                break;
        }
        dfs.stop();
    }
}
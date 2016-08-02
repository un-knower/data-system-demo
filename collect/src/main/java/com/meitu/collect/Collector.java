package com.meitu.collect;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Collector {
    public static void main(String args[]) {
        try{
            Collector col = new Collector();
            String collectDirectory = "D:\\Consume";
            String outputDirectory = "D:\\Output";
            String recoverLog = "D:\\recover.log";
            col.start(true, collectDirectory, outputDirectory, recoverLog);
            Scanner sc = new Scanner(System.in);
            while (true) {
                String command = sc.nextLine();
                if (command.equals("shutdown") || command.equals("quit") || command.equals("exit")) {
                    System.out.println("===stoping===");
                    col.stop();
                    break;
                }
            }
        }catch(Exception e){
            
        }
    }

    private boolean shutdown;

    /**
     * start collecting service
     * 
     * @throws Exception
     */
    public void start(boolean enableCheckPoint, String collectDirectory, String outputDirectory, String recoverLog) {
        this.shutdown = false;
        new Thread(new CollectorThread(enableCheckPoint, collectDirectory, outputDirectory, recoverLog)).start();
    }

    public void stop() {
        this.shutdown = true;
        System.out.println("send stop signal to CollectorThread, SinkThread and LogCheckPointThread!");
    }

    private static Logger logger = LoggerFactory.getLogger(Collector.class);

    private static class CollectInfoBlock {
        private File file;
        private boolean isCollecting;
        private long offset;

        public CollectInfoBlock(File file) {
            this(file, 0);
        }

        public CollectInfoBlock(File file, long offset) {
            this.file = file;
            this.offset = offset;
            this.isCollecting = false;
        }

        public void setCollecting(boolean isCollecting) {
            this.isCollecting = isCollecting;
        }

        public void setoffset(long offset) {
            this.offset = offset;
        }

        public File getFile() {
            return file;
        }

        public boolean isCollecting() {
            return isCollecting;
        }

        public long getoffset() {
            return offset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            else if (obj == this)
                return true;
            else if (obj.getClass() != this.getClass())
                return false;
            return this.getFile().equals(((CollectInfoBlock) obj).getFile());
        }

        @Override
        public int hashCode() {
            return this.getFile().hashCode();
        }

        @Override
        public String toString() {
            return file.getPath() + "\t" + isCollecting + "\t" + offset;
        }
    }

    private class CollectorThread implements Runnable {
        private final long LogMaxSize = 1024 * 1024 * 5;
        private boolean isMerging;
        private int sinkingNumber;
        private String collectDirectory;
        private String outputDirectory;
        private Map<String, CollectInfoBlock> watchFile;
        private String recoverLog = "";
        private long blockSize;// sink阈值
        private ExecutorService executor;
        private boolean enableCheckPoint;
        private Thread logCheckThread;

        public CollectorThread(boolean enableCheckPoint, String collectDirectory, String outputDirectory,
                String recoverLog) {
            this.collectDirectory = collectDirectory;
            this.outputDirectory = outputDirectory;
            this.watchFile = new HashMap<String, CollectInfoBlock>();
            this.isMerging = false;
            this.sinkingNumber = 0;
            this.recoverLog = recoverLog;
            this.blockSize = 1024;// 1kb
            this.executor = Executors.newCachedThreadPool();
            this.enableCheckPoint = enableCheckPoint;
        }

        synchronized private void incrSinkingNumber() {
            this.sinkingNumber++;
        }

        synchronized private void decrSinkingNumber() {
            this.sinkingNumber--;
        }

        private class LogCheckPointThread implements Runnable {
            private long checkInterval = 1000 * 60 * 5;// 5 minutes

            private void checkPoint() {
                isMerging = true;
                // System.out.println("checkpoint:等待sinkThread结束");
                try {
                    while (sinkingNumber > 0) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    // System.out.println("checkpoint:start merge");
                    // merge
                    File file = new File(recoverLog);
                    if (file.length() < LogMaxSize) {
                        PrintWriter pw = new PrintWriter(new FileOutputStream(file, true));
                        pw.println("===check point===");
                        for (Entry<String, CollectInfoBlock> entry : watchFile.entrySet()) {
                            pw.println(entry.getKey() + "\t" + entry.getValue().getoffset());
                        }
                        pw.println("===check point end===");
                        pw.close();
                    } else {
                        PrintWriter pw = new PrintWriter(new FileOutputStream(file.getPath() + ".tmp"));
                        pw.println("===check point===");
                        for (Entry<String, CollectInfoBlock> entry : watchFile.entrySet()) {
                            pw.println(entry.getKey() + "\t" + entry.getValue().getoffset());
                        }
                        pw.println("===check point end===");
                        pw.close();
                        if (file.delete()) {
                            new File(file.getPath() + ".tmp").renameTo(file);
                        }
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    isMerging = false;
                }
            }

            public void run() {
                while (!shutdown) {
                    try {
                        Thread.sleep(this.checkInterval);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        // e.printStackTrace();
                    }
                    checkPoint();
                }
            }

        }

        private class SinkThread implements Runnable {
            private CollectInfoBlock sinkBlock;
            private long aimOffset;

            public SinkThread(CollectInfoBlock sinkBlock, long aimOffset) {
                this.sinkBlock = sinkBlock;
                this.aimOffset = aimOffset;
            }

            public void run() {
                System.out.println("start sink:" + sinkBlock.getFile().getPath() + "\tfrom " + sinkBlock.offset + " to "
                        + aimOffset);
                sinkBlock.isCollecting = true;
                incrSinkingNumber();
                long realCollect = 0;
                PrintWriter pw = null;
                try {
                    pw = new PrintWriter(new FileOutputStream(recoverLog, true));// log
                                                                                 // -start
                    pw.println("start\t" + sinkBlock.getFile().getPath() + "\t" + sinkBlock.getoffset() + "\t"
                            + aimOffset);
                    pw.close();
                    InputStream in = new FileInputStream(sinkBlock.getFile());
                    OutputStream out = new FileOutputStream(
                            outputDirectory + System.getProperty("file.separator") + sinkBlock.getFile().getName(),
                            true);
                    in.skip(sinkBlock.getoffset());
                    byte[] buffer = new byte[1024 * 1024];// 1MB
                    while (true) {
                        int collectLength = in.read(buffer);
                        out.write(buffer, 0, collectLength);
                        out.flush();// flush 避免realCollect加错
                        realCollect += collectLength;
                        if (realCollect >= (aimOffset - sinkBlock.getoffset()))
                            break;
                    }
                    out.close();
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        pw = new PrintWriter(new FileOutputStream(recoverLog, true));// log
                                                                                     // -end
                        pw.println("end\t" + sinkBlock.getFile().getPath() + "\t" + sinkBlock.getoffset() + "\t"
                                + (sinkBlock.getoffset() + realCollect));
                        pw.close();
                        sinkBlock.setoffset(sinkBlock.getoffset() + realCollect);// 记录到日志成功，再设置offset；失败则不修改offset
                    } catch (FileNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    decrSinkingNumber();
                    sinkBlock.isCollecting = false;
                }
            }
        }

        private void scanCollectDirectory() {
            System.out.println("start scan");
            File file = new File(collectDirectory);
            File[] files = file.listFiles();
            for (File f : files) {
                CollectInfoBlock block = watchFile.get(f.getPath());
                if (block == null) {
                    block = new CollectInfoBlock(f);
                    watchFile.put(f.getPath(), block);
                }
                //System.out.println(f.length() + " " + block.getoffset() + " " + blockSize);
                if (!block.isCollecting && (f.length() - block.getoffset()) > blockSize) {
                    // collect
                    while (isMerging) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        if (shutdown)
                            return;
                    }
                    executor.execute(new SinkThread(block, f.length()));
                }
            }
        }

        /**
         * recoverlog pattern [start/end]\t[filePath]\t[start offset]\t[end
         * offset] filePath shouldn't contain characters{' ','\t'} eg.
         * "start\tD:\\xxx\\log\t1000\t1100"
         * 
         * check point format ===check point=== [FilePath]\t[offset] ===check
         * point end===
         * 
         * @throws Exception
         */
        private void recover() throws Exception {
            System.out.println("start recover");
            // 防止log过大操作中途失败，可能留下log.tmp文件
            File checkFile = new File(this.recoverLog);
            if (!checkFile.exists()) {
                checkFile = new File(this.recoverLog + ".tmp");
                if (!checkFile.exists()) {
                    return;// 没有log文件，什么也不用做
                }
                checkFile.renameTo(new File(this.recoverLog));
            }
            RandomAccessFile file = new RandomAccessFile(this.recoverLog, "rw");
            String line = null;
            long finalCheckPoint = -1;
            long tmpFilePointer = -1;
            try {
                while ((line = file.readLine()) != null) {// find final check
                                                          // point
                    if (line.equals("===check point===")) {
                        tmpFilePointer = file.getFilePointer();
                    } else if (line.equals("===check point end===")) {
                        if (tmpFilePointer < 0 || tmpFilePointer > file.getFilePointer())
                            throw new Exception("bad log");
                        finalCheckPoint = tmpFilePointer;
                        tmpFilePointer = -1;
                    } else
                        continue;
                }
                if (finalCheckPoint < 0)// no final check point
                    file.seek(0);
                else {// recover watchFile from final check point
                    file.seek(finalCheckPoint);
                    while (!((line = file.readLine()).equals("===check point end==="))) {
                        String[] s = line.split("\t");
                        watchFile.put(s[0], new CollectInfoBlock(new File(s[0]), Long.parseLong(s[1])));// 转换失败也是bad
                                                                                                        // log
                                                                                                        // exception
                    }
                }
                Map<String, String> badRecord = new HashMap<String, String>();
                while ((line = file.readLine()) != null) {// recover watchFile
                                                          // from log records
                    String[] s = line.split("\t");
                    if (s[0].equals("start")) {
                        badRecord.put(s[1], line);
                    } else if (s[0].equals("end")) {
                        badRecord.remove(s[1]);
                        watchFile.put(s[1], new CollectInfoBlock(new File(s[1]), Long.parseLong(s[3])));
                    } else
                        continue;// bad log
                }
                for (Entry<String, String> entry : badRecord.entrySet()) {// clean
                                                                          // bad
                                                                          // records
                    String[] s = entry.getValue().split("\t");
                    String filePath = outputDirectory + "\\" + new File(s[1]).getName();
                    File f = new File(filePath);
                    /**
                     * 说明： f.length()模拟从上报服务处查询出自s[2](start
                     * offset)开始，多少byte被写入，补充记录:end filepath startOffset start
                     * offset+已写入byte。
                     */
                    file.write(("end\t" + s[1] + "\t" + s[2] + "\t" + f.length() + "\n").getBytes());// add
                                                                                                     // end
                                                                                                     // check
                                                                                                     // point
                    watchFile.put(s[1], new CollectInfoBlock(new File(s[1]), f.length()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    file.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        public void run() {
            try {
                recover();// throw Exception("bad log"), FileNotFoundException
            } catch (Exception e1) {
                logger.info("bad log, can not recover from log");
            }
            shutdown = false;
            if (this.enableCheckPoint) {// enable check point thread
                this.logCheckThread = new Thread(new LogCheckPointThread());
                logCheckThread.start();
            }
            while (!shutdown) {// listen to collectDirectory
                scanCollectDirectory();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (this.enableCheckPoint && this.logCheckThread != null)
                this.logCheckThread.interrupt();
            executor.shutdown();
        }
    }

}
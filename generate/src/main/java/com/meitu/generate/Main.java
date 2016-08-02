package com.meitu.generate;

import java.io.File;
import java.util.Scanner;

public class Main {
    public static void main(String args[]) {
        String producePath;
        int produceThreadNum;
        String consumePath;
        if(args!=null&&args.length>=3){
            producePath = args[0];
            produceThreadNum = Integer.parseInt(args[1]);
            consumePath = args[2];
        }
        else
            return;
        
        //为了测试方便，清理生产路径
        File file = new File(producePath);
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.getName().length() > 5
                    && f.getName().substring(f.getName().length() - 5, f.getName().length()).equals("-done"))
                f.renameTo(new File(f.getPath().substring(0, f.getPath().length() - 5)));
            if (f.getName().length() > 5
                    && f.getName().substring(f.getName().length() - 5, f.getName().length()).equals("-lock"))
                f.renameTo(new File(f.getPath().substring(0, f.getPath().length() - 5)));
        }
        
        Producer producer = new Producer(producePath,produceThreadNum,consumePath);
        producer.start();
        Scanner sc = new Scanner(System.in);
        while (true) {
            String command = sc.nextLine();
            if (command.equals("shutdown") || command.equals("quit") || command.equals("exit")) {
                System.out.println("===stoping===");
                producer.stop();
                // System.out.println("===stop success===");
                break;
            }
        }
    }
}
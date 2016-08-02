package com.meitu.generate;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Scanner;

public class TestFileGenerator {
    public static void main(String args[]) throws FileNotFoundException {
        String path;
        int fileRecordNum = 10000;
        int fileNum = 60;
        int times = 1000;
        if (args == null || args.length < 1) {
            System.out.println("path please:");
            Scanner sc = new Scanner(System.in);
            path = sc.nextLine();
            sc.close();
            if (path.equals(""))
                path = "D:\\Produce";
        } else {
            path = args[0];
        }
        for (int i = 0; i < fileNum; i++) {
            PrintWriter pw = new PrintWriter(new FileOutputStream(path + "\\test-" + i, true));
            for (int j = fileRecordNum * i; j < fileRecordNum * i + fileRecordNum; j++) {
                for (int k = 0; k < times; k++)
                    pw.print(j);
                pw.println();
            }
            pw.close();
        }
    }

}
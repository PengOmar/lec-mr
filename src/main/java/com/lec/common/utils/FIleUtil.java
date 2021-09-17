package com.lec.common.utils;

import org.apache.commons.io.output.FileWriterWithEncoding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

public class FIleUtil {

    public static void generateFile(String targetPath, String sql) throws IOException {
        //如果已经存在相同文件则删除
        File targetFile = new File(targetPath);
        targetFile.delete();
        writtenFile(targetFile, sql);
    }

    //写文件
    static void writtenFile(File file, String sql) throws IOException {
        //写文件
        file.createNewFile();
        FileWriterWithEncoding fw = new FileWriterWithEncoding(file, "utf-8", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(sql);
        bw.flush();
        fw.flush();
        System.out.println("done");


    }
}

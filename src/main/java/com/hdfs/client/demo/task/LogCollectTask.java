package com.hdfs.client.demo.task;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @Author : peter
 * @Date: 2018-09-13 20:10
 * @Description: 日志采集任务
 * @Copyright(©) 2018 by peter.
 */
@Slf4j
public class LogCollectTask implements Job {

    @Autowired
    private FsShell fsShell;

    private final String ACCESS_LOG_DIR="F:/hadoop/logs/accesslog/";

    private final String TO_UPLOAD_LOG_DIR="F:/hadoop/logs/toupload";

    private final String BAKUP_LOG_DIR="F:/hadoop/logs/bakup/";


    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        log.info("job start....");
        String currentDate = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date());
        //定义日志源目录
        File srcDir = new File(ACCESS_LOG_DIR);
        if(srcDir.exists()){
            srcDir.mkdirs();
        }
        File[] listFiles = srcDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("access.log.");
            }
        });
        //将日志拷贝到待上传目录中
        File uploadDir = new File(TO_UPLOAD_LOG_DIR);
        Lists.newArrayList(listFiles).stream().forEach(f -> {
            try {
                FileUtils.moveToDirectory(f,uploadDir,true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        //将文件从待上传目录拷贝到HDFS中
        Lists.newArrayList(uploadDir.listFiles()).stream().forEach(f -> {
            //上传到HDFS
            fsShell.put(f.getAbsolutePath(),"/test/"+currentDate+"/access_log_"+ UUID.randomUUID().toString()+".log");
            log.info("上传日志文件{}到目录{}成功",f.getAbsolutePath(),"/test/"+currentDate+"/");
            //上传成功，将日志文件移动至备份目录
            try {
                FileUtils.moveToDirectory(f,new File(BAKUP_LOG_DIR+currentDate),true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("备份日志文件{}到目录{}成功",f.getAbsolutePath(),BAKUP_LOG_DIR);
        });

        try {
            Thread.sleep(60*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.info("job end....");
    }
}

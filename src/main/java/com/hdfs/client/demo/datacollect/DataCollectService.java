package com.hdfs.client.demo.datacollect;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @Author : peter
 * @Date: 2018-09-11 19:59
 * @Description: 数据采集
 * @Copyright(©) 2018 by peter.
 */
@Slf4j
@Component
public class DataCollectService {

    private final String ACCESS_LOG_DIR="F:/hadoop/logs/accesslog/";

    private final String TO_UPLOAD_LOG_DIR="F:/hadoop/logs/toupload";

    private final String BAKUP_LOG_DIR="F:/hadoop/logs/bakup/";


    @Autowired
    private FsShell fsShell;

//    @Scheduled(cron = "0 0/60 * * * ?")
//    public void collect(){
//        log.info("long Min:{}",String.valueOf(Long.MIN_VALUE).length());
//        log.info("long Max:{}",String.valueOf(Long.MAX_VALUE).length());
//        log.info("每60分钟执行一次");
//        String currentDate = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date());
//        //定义日志源目录
//        File srcDir = new File(ACCESS_LOG_DIR);
//        if(srcDir.exists()){
//            srcDir.mkdirs();
//        }
//        File[] listFiles = srcDir.listFiles(new FilenameFilter() {
//            @Override
//            public boolean accept(File dir, String name) {
//                return name.startsWith("access.log.");
//            }
//        });
//        //将日志拷贝到待上传目录中
//        File uploadDir = new File(TO_UPLOAD_LOG_DIR);
//        Lists.newArrayList(listFiles).stream().forEach(f -> {
//            try {
//                FileUtils.moveToDirectory(f,uploadDir,true);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        //将文件从待上传目录拷贝到HDFS中
//        Lists.newArrayList(uploadDir.listFiles()).stream().forEach(f -> {
//            //上传到HDFS
//            fsShell.put(f.getAbsolutePath(),"/test/"+currentDate+"/access_log_"+ UUID.randomUUID().toString()+".log");
//            log.info("上传日志文件{}到目录{}成功",f.getAbsolutePath(),"/test/"+currentDate+"/");
//            //上传成功，将日志文件移动至备份目录
//            try {
//                FileUtils.moveToDirectory(f,new File(BAKUP_LOG_DIR+currentDate),true);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            log.info("备份日志文件{}到目录{}成功",f.getAbsolutePath(),BAKUP_LOG_DIR);
//        });
//    }


    //超过24小时的备份日志自动删除
    @Scheduled(initialDelay = 0,fixedDelay = 60*60*1000)
    public void bakUpClean(){
        log.info("log clean start...");
        File bakupDir = new File(BAKUP_LOG_DIR);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");
        Lists.newArrayList(bakupDir.listFiles()).stream().forEach(file -> {
            if (LocalDateTime.now().isAfter(LocalDateTime.parse(file.getName(),formatter).plusHours(24))){
                try {

                    FileUtils.deleteDirectory(file);
                    log.info("删除日志备份目录成功:{}",file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public static void main(String[] args){
        String date = "2018-09-12 21";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");

        System.out.println(LocalDateTime.parse(date,formatter));
        System.out.println(LocalDateTime.now().isAfter(LocalDateTime.parse(date,formatter).plusHours(24)));


        List<String> dataList = Lists.newArrayList("aa","bb","cc","dd");
        System.out.println("dataList.size:{}"+dataList.size());
        String username="aa";
        ImmutableList<String> of = ImmutableList.of("a", "b", "c", "d");

        ImmutableMap<String,String> map = ImmutableMap.of("key1", "value1", "key2", "value2");

        CharMatcher.is('a');
        dataList.stream().filter(str -> username.equalsIgnoreCase(str)).forEach(s -> System.out.println(s));
    }


}

package com.hdfs.client.demo.datacollect;

import java.util.Timer;

/**
 * @Author : peter
 * @Date: 2018-09-11 19:59
 * @Description: 数据采集
 * @Copyright(©) 2018 by peter.
 */
public class DataCollectService {

    public void collect(){

        Timer timer = new Timer();
        timer.schedule(null,0,60*60*1000L);

    }


}

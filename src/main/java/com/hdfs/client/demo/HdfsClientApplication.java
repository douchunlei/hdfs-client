package com.hdfs.client.demo;

import com.hdfs.client.demo.task.LogCollectTask;
import org.quartz.*;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@EnableScheduling
@SpringBootApplication
@RestController
public class HdfsClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(HdfsClientApplication.class, args);
	}

	@Autowired
	private Scheduler scheduler;


	@GetMapping("/start/job")
	public String startJob() throws SchedulerException {
		JobDetail jobDetail = JobBuilder.newJob(LogCollectTask.class).build();
		//corn表达式  每2秒执行一次
		CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule("0/30 * * * * ?");
		//设置定时任务的时间触发规则
		CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity("日志采集","10001") .withSchedule(scheduleBuilder).build();
		SimpleTrigger simpleTrigger = new SimpleTriggerImpl("日志采集","10001");
		scheduler.scheduleJob(jobDetail,simpleTrigger);
		return "start job";
	}

	@GetMapping("/pause/job")
	public String pauseJob() throws SchedulerException {
		scheduler.pauseJob(new JobKey("日志采集","10001"));
		return "pause job";
	}

	@GetMapping("/stop/job")
	public String stopJob() throws SchedulerException {
		scheduler.deleteJob(new JobKey("日志采集","10001"));
		return "stop job";
	}

}

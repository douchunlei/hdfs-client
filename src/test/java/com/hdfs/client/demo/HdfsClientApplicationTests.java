package com.hdfs.client.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URI;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsClientApplicationTests {

	@Autowired
	FsShell shell;

	private FileSystem fileSystem;

	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "4");
		conf.set("dfs.blocksize", "64m");
		fileSystem = FileSystem.get(new URI("hdfs://hdp-129:9000/"), conf, "root");
	}


	@Test
	public void contextLoads() throws Exception {
		shell.put("E:\\开发工具\\jdk_8.0.1310.11_64.zip","/test/dcl/jdk212.zip");

//
//		fileSystem.copyFromLocalFile(new Path("E:\\开发工具\\jdk_8.0.1310.11_64.zip"),new Path("/jdk.tar.gz"));

		System.out.println("=========run start============");
		for (FileStatus fileStatus : shell.lsr("/")) {
			System.out.println(">" + fileStatus.getPath());
		}
		System.out.println("===========run end===========");

		//
	}

	@Test
	public void testMkdir(){
		shell.mkdir("/test/log");
	}

	@Test
	public void testUpload(){
		shell.put("E:\\开发工具\\jdk_8.0.1310.11_64.zip","/test/log/");
	}

	@Test
	public void testDelete(){
//		fileSystem.delete(new Path("/hadoop-root-datanode-hdp-190.log.bak"),false);
//		fileSystem.close();
		shell.rm("/test/log/jdk_8.0.1310.11_64.zip");
	}

	@Test
	public void testDownload() throws Exception {
		fileSystem.copyToLocalFile(new Path("/hadoop-root-datanode-hdp-190.log"),new Path("E:\\开发工具\\hadoop-root-datanode-hdp-190.log"));
		fileSystem.close();
	}

}

package com.axing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * hdfs客户端
 */
public class HdfsClientTests {

    static String PATH = "/xiyouji/huaguoshan";
    FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建配置文件
        Configuration configuration = new Configuration();
        // 用户
        String user = "admin";
        // 获取客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 关闭资源
        fileSystem.close();
    }

    @Test
    public void testMkdir() throws IOException {
        // 创建文件夹
        fileSystem.mkdirs(new Path(PATH));
    }

    @Test
    public void delete() throws IOException {
        // 删除文件
        // 参数1:文件路径,参数2:是否递归
        //非空文件,非递归,无法删除
        fileSystem.delete(new Path(PATH), true);
    }


    /**
     * @return void
     * @Description 上传
     * @Date 2023/3/15 22:32
     */
    @Test
    public void testPut() throws IOException {
        // 参数一:表示删除原数据;参数二:是否允许覆盖;参数三:原数据路径;参数四:目的地路径
        fileSystem.copyFromLocalFile(false,
                true,
                new Path("./sun.txt"),
                new Path(PATH));
    }

    /**
     * @return void
     * @Description 文件下载
     * @Date 2023/3/15 23:15
     */
    @Test
    public void testGet() throws IOException {
        //参数一:原文件是否删除，参数二:原文件路径HDFS;参数三:目标地址路径Win ;参数四:是否关闭文件校验
        fileSystem.copyToLocalFile(false,
                new Path(PATH),
                new Path("./download"),
                false);
    }


    /**
     * @return void
     * @Description 文件更名和移动
     * @Date 2023/3/15 23:15
     */
    @Test
    public void testMv() throws IOException {
        // 参数一:源文件路径；参数二:目标文件路径;
        // 对文件名称的修改
//        fileSystem.rename(new Path("/input/word.txt") ,new Path("input/66.txt"));

        // 文件的移动和更名
//        fileSystem.rename(new Path("/input/66.txt") ,new Path("/77.txt"));

        // 目录更名
        fileSystem.rename(new Path("/input"), new Path("/output.txt"));
    }


    /**
     * @return void
     * @Description 获取文件详细信息
     * @Date 2023/3/15 23:15
     */
    @Test
    public void testListFiles() throws IOException {
        // 获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        // 遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("==========" + fileStatus.getPath() + "==========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }


    /**
     * @return void
     * @Description 文件和文件夹判断
     * @Date 2023/3/15 23:15
     */
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件" + status.getPath().getName());
            } else {
                System.out.println("目录" + status.getPath().getName());
            }
        }
    }

}

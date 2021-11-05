package top.rabbitcrows.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author LEHOSO
 * @date 2021/10/18
 * @apinote
 */
public class HDFS_CRUD {

    FileSystem fs = null;

    //初始化客户端对象
    @Before
    public void init() throws Exception {
        //构造一个配置参数对象，设置一个参数：要访问的HDFS的URI
        Configuration conf = new Configuration();
        //这里指定使用的是HDFS
        conf.set("fs.defaultFS", "hdfs://192.168.100.10:9000");
        //通过如下的方式进行客户端身份的设置
        System.setProperty("HADOOP_USER_NAME", "root");
        //通过FileSystem的静态方法获取文件系统客户端对象
        fs = FileSystem.get(conf);
    }

    //上传文件到HDFS
    @Test
    public void testAddFileToHdfs() throws IOException {
        //要上传的文件所在本地路径
        Path src = new Path("D:/test.txt");
        //要上传到HDFS目标路径
        Path dst = new Path("/testFile");
        //上传文件方法调用
        fs.copyFromLocalFile(src, dst);
        //关闭资源
        fs.close();
    }

    //从HDFS中复制文件到本地系统
    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        //下载文件
        /**
         * 在windows平台开发HDFS项目时候，不设置Hadoop开发环境，会报错
         * 安装hadoop环境，需要重启IDEA或者重启电脑才能生效
         * 需要相关依赖：winuitls.exe、winutis.pdb、hadoop.dll
         */
        fs.copyToLocalFile(false,
                new Path("/testFile"),
                new Path("E:/x.txt"),
                true);
        /**
         * delSrc参数设置为False
         * useRawLocalFileSystem参数设置为true
         * 下载的文件就没有 带.crc的校验文件
         */
        fs.close();
    }

    //创建、删除、重命名文件
    @Test
    public void testMkdirAndDeleteAndRename() throws Exception {
        //创建项目
        fs.mkdirs(new Path("/a/b/c"));
        fs.mkdirs(new Path("/a2/b2/c2"));
        //重命名文件或文件夹
        fs.rename(new Path("/a"), new Path("/a3"));
        //删除文件夹，如果是非空文件夹，参数2【recurisive】值必须true
        fs.delete(new Path("/a2"), true);
    }

    //查看目录信息，只显示文件
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        //获取迭代器对象
        /**ListFile方法
         * param pathstring：为路径
         * param recursive：为是否为递归查询
         */
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            //打印当前文件名
            System.out.println(fileStatus.getPath().getName());
            ///打印当前块大小
            System.out.println(fileStatus.getBlockSize());
            //打印当前文件权限
            System.out.println(fileStatus.getPermission());
            //打印当前文件内容长度
            System.out.println(fileStatus.getLen());
            //获取该文件块信息（包含长度，数据块，datanode的信息）
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("---------分割线---------");
            //若测试显示信息太多，可以删除或者禁用log4j
        }
    }
}

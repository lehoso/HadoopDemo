package top.rabbitcrows.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * @author LEHOSO
 * @date 2021/11/21
 * @apinote
 */
public class ZookeeperTest {

    public static void main(String[] args) throws Exception {
        // 初始化ZooKeeper实例(zk地址、会话超时时间，与系统默认一致, watcher)
        //步骤一：创建Zookeeper客户端
        //参数1：zk地址；参数2：会话超时时间（与系统默认一致）;参数3：监视器
        ZooKeeper zk = new ZooKeeper(
                "192.168.142.10:2181," +
                        "192.168.142.20:2181," +
                        "192.168.142.30:2181", 300000, new Watcher() {
            @Override
            //监控所有被触发的事件（也就是在这里进行事件的处理）
            public void process(WatchedEvent watchedEvent) {
                System.out.println("事件类型为：" + watchedEvent.getType());
                System.out.println("事件发生的路径：" + watchedEvent.getPath());
                System.out.println("通知状态为：" + watchedEvent.getState());
            }
        });

        //步骤二：创建目录节点
        //参数1：要创建的节点路径；参数2：节点数据；参数3：节点权限;参数4：节点类型
        zk.create("/testRootPath", "testRootData".getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //步骤三：创建子目录节点
        zk.create("/testRootPath/testChildPathOne", "testChildPathOne".getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //步骤四：获取目录节点数据
        //参数1：存储节点数据的路径；
        //参数2：是否需要监控此节点（true/false）
        //参数3：stat节点的统计信息（一般设置null）
        System.out.println("testRootData节点数据为：" +
                new String(zk.getData("/testRootPath", false, null)));
        //步骤五：获取子目录节点数据
        // 取出子目录节点列表
        System.out.println(zk.getChildren("/testRootPath", true));

        //步骤六：修改子目录节点数据，使得监听触发
        //参数1：存储子目录节点数据的路径;
        //参数2：要修改的数据；
        //参数3：预期要匹配的版本（设置为-1，则可匹配任何节点的版本）
        zk.setData("/testRootPath/testChildPathOne", "modifyChildDataOne".getBytes(), -1);
        //步骤七：判断目录节点是否存在
        System.out.println("目录节点状态[" + zk.exists("/testRootPath", true) + "]");

        //步骤八：删除子目录节点
        zk.delete("/testRootPath/testChildPathOne", -1);
        //步骤九：删除目录节点
        zk.delete("/testRootPath", -1);
        zk.close();
    }
}

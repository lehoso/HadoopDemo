package top.rabbitcrows.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @author LEHOSO
 * @date 2021/11/21
 * @apinote
 */
public class Test {


    private static String ip = "192.168.142.10:2181";
    private static int session_timeout = 40000;
    private static CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ip, session_timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    //确认已经连接完毕后再进行操作
                    latch.countDown();
                    System.out.println("已经获得了连接");
                }
            }
        });

        //连接完成之前先等待
        latch.await();
        ZooKeeper.States states = zooKeeper.getState();
        System.out.println(states);
    }

}

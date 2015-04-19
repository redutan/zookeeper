package redutan.study.zookeeper.chapter3;

import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master implements Watcher  {
	
	final Logger log = LoggerFactory.getLogger(this.getClass()); 
	
	Random random = new Random();
	
	ZooKeeper zk;
	String hostPort;
	String serverId = Integer.toHexString(random.nextInt());
	static boolean isLeader = false;
	
	Master(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() throws Exception {
		zk = new ZooKeeper(hostPort, 15000, this);
	}
	
	void stopZK() throws Exception {
		zk.close();
	}
	
	@Override
	public void process(WatchedEvent e) {
		log.info("{}", e);
	}
	
	boolean checkMaster() throws InterruptedException, KeeperException {
		while (true) {
			try {
				Stat stat = new Stat();
				byte data[] = zk.getData("/master", false, stat);
				isLeader = new String(data).equals(serverId);
				return true;
			} catch (NoNodeException e) {
				// 마스터가 없으므로 생성을 재시도한다.
				return false;
			} catch (ConnectionLossException e) {
				
			}
		}
	}
	
	void runForMaster() throws InterruptedException, KeeperException {
		while (true) {
			try {
				zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				isLeader = true;
				break;
			} catch (NodeExistsException e) {
				isLeader = false;
				break;
			} catch (ConnectionLossException e) {
				
			}
			if (checkMaster()) break;
		}
	}
	
	public static void main(String args[]) throws Exception {
		String hostPort = "127.0.0.1:2181";
		if (args.length > 0) {
			hostPort = args[0];
		}
		Master m = new Master(hostPort);
		m.startZK();
		
		m.runForMaster();
		
		if (isLeader) {
			System.out.println("I'm the leader");
			// 60초 대기시킨다.
			Thread.sleep(60000);
		} else {
			System.out.println("Someone else is the leader");
		}
		
		
		m.stopZK();
	}
}

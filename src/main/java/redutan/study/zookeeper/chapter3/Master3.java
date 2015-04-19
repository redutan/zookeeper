package redutan.study.zookeeper.chapter3;

import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master3 implements Watcher  {
	
	final Logger log = LoggerFactory.getLogger(this.getClass()); 
	
	Random random = new Random();
	
	ZooKeeper zk;
	String hostPort;
	String serverId = Integer.toHexString(random.nextInt());
	static boolean isLeader = false;
	
	StringCallback masterCreateCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS : 
				checkMaster();
				return;
			case OK : 
				isLeader = true;
				break;
			default :
				isLeader = false;
			}
			log.info("I'm " + (isLeader ? "" : "not ") + "the leader");
		}
	};
	
	DataCallback masterCheckCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS :
				checkMaster();
				return;
			case NONODE :
				runForMaster();
				return;
			}
		}
	};
	
	StringCallback createParentCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS :
				createParent(path, (byte[]) ctx);
				break;
			case OK :
				log.info("Parent created");
				break;
			case NODEEXISTS :
				log.warn("Parent already registered : {}" , path);
				break;
			default :
				log.error("Somthing went wrong : {}", KeeperException.create(Code.get(rc), path));
			}
		}
	};
	
	Master3(String hostPort) {
		this.hostPort = hostPort;
	}
	
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}
	
	void createParent(String path, byte[] data) {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
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
	
	void checkMaster() {
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
	void runForMaster() {
		zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
	}
	
	public static void main(String args[]) throws Exception {
		String hostPort = "127.0.0.1:2181";
		if (args.length > 0) {
			hostPort = args[0];
		}
		Master3 m = new Master3(hostPort);
		m.startZK();
		m.bootstrap();
		m.runForMaster();
		
		Thread.sleep(60000);
		
		m.stopZK();
	}
}

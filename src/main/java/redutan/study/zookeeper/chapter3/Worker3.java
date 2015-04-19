package redutan.study.zookeeper.chapter3;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker3 implements Watcher {
	
	private static final Logger LOG = LoggerFactory.getLogger(Worker3.class);
	
	Random random = new Random();
	
	ZooKeeper zk;
	String hostPort;
	String serverId = Integer.toHexString(random.nextInt());
	
	Worker3(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}
	
	void register() {
		zk.create("/workers/worker-" + serverId, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
	}
	
	StringCallback createWorkerCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS :
				register();
				break;
			case OK :
				LOG.info("Registered successfully: {}", serverId);
				break;
			case NODEEXISTS :
				LOG.warn("Already registered : {}", serverId);
				break;
			default :
				LOG.error("Something went wrong: {}", KeeperException.create(Code.get(rc), path).toString());
			}
		}
		
	};
	
	@Override
	public void process(WatchedEvent event) {
		LOG.info("{}, {}", event, hostPort);
	}
	
	
	
	public static void main(String args[]) throws Exception {
		String hostPort = "127.0.0.1:2181";
		if (args.length > 1) {
			hostPort = args[0];
		}
		Worker3 w = new Worker3(hostPort);
		w.startZK();
		
		w.register();
		
		Thread.sleep(30000);
	}

}

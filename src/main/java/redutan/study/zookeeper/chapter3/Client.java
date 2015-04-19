package redutan.study.zookeeper.chapter3;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Client implements Watcher {
	
	ZooKeeper zk;
	String hostPort;
	
	Client(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK() throws Exception {
		zk = new ZooKeeper(hostPort, 15000, this);
	}
	
	String queueCommand(String command) throws Exception {
		while (true) {
			String name = null;
			try {
				name = zk.create("/tasks/task-", command.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				return name;
			} catch (NodeExistsException e) {
				throw new Exception(name + " already appears to be running");
			} catch (ConnectionLossException e) {
				
			}
		}	
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}
	
	public static void main(String args[]) throws Exception {
		String hostPort = "127.0.0.1:2181";
		String command = "test";
		if (args.length > 0) {
			hostPort = args[0];
		}
		if (args.length > 1) {
			command = args[1];
		}
		Client c = new Client(hostPort);
		
		c.startZK();
		
		String name = c.queueCommand(command);
		System.out.println("Created " + name);
	}

}

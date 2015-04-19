package redutan.study.zookeeper.chapter3;

import java.util.Date;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AdminClient implements Watcher {
	
	ZooKeeper zk;
	String hostPort;
	
	AdminClient(String hostPort) throws Exception {
		zk = new ZooKeeper(hostPort, 15000, this);
	}
	
	void listState() throws Exception {
		
		try{
			Stat stat = new Stat();
			byte[] masterData = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime());
			System.out.println("Master : " + new String(masterData) + " since " + startDate);
		} catch (NoNodeException e) {
			System.out.println("No Master");
		}
		
		System.out.println("Workers:");
		for (String w : zk.getChildren("/workers", false)) {
			byte[] data = zk.getData("/workers/" + w, false, null);
			String state = new String(data);
			System.out.println("\t" + w + " : " + state);
		}
		
		System.out.println("Tasks:");
		for (String t : zk.getChildren("/tasks", false)) {
			System.out.println("\t" + t);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}
	
	public static void main(String args[]) throws Exception {
		String hostPort = "127.0.0.1:2181";
		if (args.length > 0) {
			hostPort = args[0];
		}
		AdminClient c = new AdminClient(hostPort);
		//c.start();
		c.listState();
	}

}

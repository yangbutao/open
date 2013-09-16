package org.open.zookeeper.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式锁应用示例
 * 多个进程节点的leader选举
 * @author yangbutao
 * 
 */
public class ElectionMain {
	private static Logger log = LoggerFactory.getLogger(ElectionMain.class);

	private final static Pattern LEADER_SEQ = Pattern
			.compile(".*?/?.*?-n_(\\d+)");
	private final static Pattern SESSION_ID = Pattern
			.compile(".*?/?(.*?-.*?)-n_\\d+");

	public static void main(String[] args) throws Exception {
		ZooKeeper zkClient = new ZooKeeper("10.1.1.20:2222", 3000, null);
		String electionPath = "/election";
		// 当前节点名称
		String coreNodeName = "192.168.1.111:8983";
		// 客户端节点的sessionId
		long sessionId = zkClient.getSessionId();
		String id = sessionId + "-" + coreNodeName;
		String leaderSeqPath = null;
		boolean cont = true;
		int tries = 0;
		while (cont) {
			try {
				leaderSeqPath = zkClient.create(
						electionPath + "/" + id + "-n_", null,
						ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
				cont = false;
			} catch (ConnectionLossException e) {
				List<String> entries = zkClient.getChildren(electionPath, true);
				boolean foundId = false;
				for (String entry : entries) {
					String nodeId = getNodeId(entry);
					if (id.equals(nodeId)) {
						foundId = true;
						break;
					}
				}
				if (!foundId) {
					cont = true;
					if (tries++ > 20) {
						throw new Exception("server error", e);
					}
					try {
						Thread.sleep(50);
					} catch (InterruptedException e2) {
						Thread.currentThread().interrupt();
					}
				}

			} catch (KeeperException.NoNodeException e) {
				if (tries++ > 20) {
					throw new Exception("server error", e);
				}
				cont = true;
				try {
					Thread.sleep(50);
				} catch (InterruptedException e2) {
					Thread.currentThread().interrupt();
				}
			}
		}
		int seq = getSeq(leaderSeqPath);
		checkIfIamLeader(zkClient, seq);
	}

	private static String getNodeId(String nStringSequence) {
		String id;
		Matcher m = SESSION_ID.matcher(nStringSequence);
		if (m.matches()) {
			id = m.group(1);
		} else {
			throw new IllegalStateException("Could not find regex match in:"
					+ nStringSequence);
		}
		return id;
	}

	private static int getSeq(String nStringSequence) {
		int seq = 0;
		Matcher m = LEADER_SEQ.matcher(nStringSequence);
		if (m.matches()) {
			seq = Integer.parseInt(m.group(1));
		} else {
			throw new IllegalStateException("Could not find regex match in:"
					+ nStringSequence);
		}
		return seq;
	}

	/**
	 * 排序seq
	 */
	private static void sortSeqs(List<String> seqs) {
		Collections.sort(seqs, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Integer.valueOf(getSeq(o1)).compareTo(
						Integer.valueOf(getSeq(o2)));
			}
		});
	}

	private static List<Integer> getSeqs(List<String> seqs) {
		List<Integer> intSeqs = new ArrayList<Integer>(seqs.size());
		for (String seq : seqs) {
			intSeqs.add(getSeq(seq));
		}
		return intSeqs;
	}

	private static void checkIfIamLeader(final ZooKeeper zkClient, final int seq)
			throws KeeperException, InterruptedException, IOException {
		// get all other numbers...
		final String holdElectionPath = "/election";
		List<String> seqs = zkClient.getChildren(holdElectionPath, true);
		sortSeqs(seqs);
		List<Integer> intSeqs = getSeqs(seqs);
		if (intSeqs.size() == 0) {
			return;
		}
		if (seq <= intSeqs.get(0)) {
			// 删除来的leader节点
			try {
				zkClient.delete("/leader", -1);
			} catch (Exception e) {
				// fine
			}
			String seqStr = null;
			for (String currSeq : seqs) {
				if (getSeq(currSeq) == seq) {
					seqStr = currSeq;
					break;
				}
			}
			runIamLeaderProcess(zkClient, seqStr);
		} else {
			// I am not the leader - watch the node below me
			// 当前节点不是leader，watcher比我小的节点
			int i = 1;
			for (; i < intSeqs.size(); i++) {
				int s = intSeqs.get(i);
				if (seq < s) {
					// we found who we come before - watch the guy in front
					// 发现比我小的节点(节点列表全面经过排序)，退出循环
					break;
				}
			}
			int index = i - 2;
			if (index < 0) {
				log.warn("Our node is no longer in line to be leader");
				return;
			}
			try {
				// 监控比当前节点seq次小的节点的值变化
				zkClient.getData(holdElectionPath + "/" + seqs.get(index),
						new Watcher() {

							@Override
							public void process(WatchedEvent event) {
								if (EventType.None.equals(event.getType())) {
									return;
								}
								// 检查是否是可以做为leader
								try {
									checkIfIamLeader(zkClient, seq);
								} catch (InterruptedException e) {
									Thread.currentThread().interrupt();
									log.warn("", e);
								} catch (IOException e) {
									log.warn("", e);
								} catch (Exception e) {
									log.warn("", e);
								}
							}

						}, null, true);
			} catch (Exception e) {
				log.warn("Failed setting watch", e);
				checkIfIamLeader(zkClient, seq);
			}
		}
	}

	protected static void runIamLeaderProcess(ZooKeeper zkClient, String seqStr)
			throws KeeperException, InterruptedException, IOException {
		final String id = seqStr.substring(seqStr.lastIndexOf("/") + 1);
		// 设置leader节点
		zkClient.create("/leader", id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
	}

}

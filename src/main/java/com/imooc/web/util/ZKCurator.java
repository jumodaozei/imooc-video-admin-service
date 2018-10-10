package com.imooc.web.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKCurator {
	private CuratorFramework client = null;
	
	final static Logger log = LoggerFactory.getLogger(ZKCurator.class);
	
	public ZKCurator(CuratorFramework client) {
		this.client = client;
	}
	
	public void init() {
		client =  client.usingNamespace("admin");
		try {
			//判断在admin命名空间下是否有bgm节点 /admin/bgm
			if(client.checkExists().forPath("/bgm") == null) {
				/**
				 * zk有两种节点
				 * 持久节点：创建后就一直存在 除非手工删除
				 * 临时节点：创建后，绘画断开 就会删除
				 * acl:匿名权限
				 */
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(Ids.OPEN_ACL_UNSAFE).forPath("/bgm");
				log.info("zk客户端初始化成功！");
				log.info("zookeeper服务器状态：{}",client.isStarted());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("zookeeper客户端连接初始化错误！");
			e.printStackTrace();
		}
	}
	
	/**
	 * 增加|删除 bgm 向zk-server创建子节点 供小程序后端监听
	 * @param bgmId
	 * @param operType
	 */
	public void sendBgmOperator(String bgmId,String operObj) {
		try {
			client.create().creatingParentsIfNeeded().
			withMode(CreateMode.PERSISTENT).
			withACL(Ids.OPEN_ACL_UNSAFE).
			forPath("/bgm/" + bgmId,operObj.getBytes());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

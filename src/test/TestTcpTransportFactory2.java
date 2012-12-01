package test;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class TestTcpTransportFactory2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RemoteCacheManager container = new RemoteCacheManager();
		RemoteCache<String, Object> cache = container.getCache("left");
		cache.put("2", "222");
		
		Object val=cache.get("2");
		System.out.println(val);
		cache.stop();
	}

}

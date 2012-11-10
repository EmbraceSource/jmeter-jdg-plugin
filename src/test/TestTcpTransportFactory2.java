package test;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class TestTcpTransportFactory2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RemoteCacheManager container = new RemoteCacheManager();
		RemoteCache<String, Object> cache = container.getCache();
		cache.put("1", "1");
		cache.stop();
	}

}

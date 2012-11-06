package org.test.hotrod;

import java.util.Random;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.infinispan.api.BasicCache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class HotRod extends AbstractJavaSamplerClient {
	private static BasicCacheContainer container = new RemoteCacheManager();
	private String cacheName = null;
	private String putOrGet;
	private String value;

	@Override
	public void setupTest(JavaSamplerContext context) {
		super.setupTest(context);
		cacheName = context.getParameter("cacheName", "");
		putOrGet = context.getParameter("putOrGet", "put");
		int size = context.getIntParameter("size", 1024);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size; i++) {
			sb.append(i % 100);
		}
		value = sb.toString();
		System.out.println("cacheName : " + cacheName);
		System.out.println("size : " + size);
		System.out.println("putOrGet : " + putOrGet);
	}

	@Override
	public void teardownTest(JavaSamplerContext context) {
		super.teardownTest(context);
		container.stop();
	}

	@Override
	public SampleResult runTest(JavaSamplerContext arg0) {
		SampleResult sr = new SampleResult();
		sr.setSampleLabel("infinispan java rod hot");
		BasicCache<String, Object> cache = null;
		if (!cacheName.equals("")) {
			cache = container.getCache(cacheName);
		} else {
			cache = container.getCache();
		}
		String key = Thread.currentThread().getName()
				+ new Random().nextInt(100000);
		try { // 这里调用我们要测试的java类，这里我调用的是一个Test类
			sr.sampleStart(); // 记录程序执行时间，以及执行结果
			if (putOrGet.equals("put")) {
				cache.put(key, value);
			} else {
				cache.get(key);
			}
			sr.setSuccessful(true);
		} catch (Throwable e) {
			System.out.println("Exception is " + e.getMessage());
			sr.setSuccessful(false);
		} finally {
			sr.sampleEnd();
		}
		return sr;
	}

	public Arguments getDefaultParameters() {
		Arguments params = new Arguments();
		params.addArgument("cacheName", "");
		params.addArgument("size", "1024");
		params.addArgument("putOrGet", "put");

		return params;

	}

}

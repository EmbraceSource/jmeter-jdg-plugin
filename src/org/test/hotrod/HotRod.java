package org.test.hotrod;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;

public class HotRod extends AbstractJavaSamplerClient {
	private static RemoteCacheManager container = null;
	private static Lock lock = new ReentrantLock();
	private String cacheName = null;
	private String putOrGet;
	private String value;
	private Integer keyLength;
	private Integer keyRang;

	@Override
	public void setupTest(JavaSamplerContext context) {
		super.setupTest(context);

		if (container == null) {
			try {
				lock.lock();
				if (container == null) {
					container = new RemoteCacheManager();
					System.out.println("init container");
				}
			} finally {
				lock.unlock();
			}
		}

		cacheName = context.getParameter("cacheName", "");
		putOrGet = context.getParameter("putOrGet", "put");
		keyLength = context.getIntParameter("keyLength", 150);
		keyRang = context.getIntParameter("keyRang", 100000);
		int size = context.getIntParameter("size", 1024);

		if (value == null) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < size; i++) {
				sb.append(i % 100);
			}
			value = sb.toString();
		}
	}

	@Override
	public void teardownTest(JavaSamplerContext context) {
		super.teardownTest(context);
		System.out.println("stop container");
		container.stop();
		container=null;
	}

	public String repeat(char ch, int repeat) {
		char[] buf = new char[repeat];
		for (int i = repeat - 1; i >= 0; i--) {
			buf[i] = ch;
		}
		return new String(buf);
	}

	@Override
	public SampleResult runTest(JavaSamplerContext arg0) {
		SampleResult sr = new SampleResult();
		sr.setSampleLabel("hotrod-" + putOrGet);
		RemoteCache<String, Object> cache = null;
		if (!cacheName.equals("")) {
			cache = container.getCache(cacheName);
		} else {
			cache = container.getCache();
		}
		int in = new Random().nextInt(keyRang);
		String key = repeat('a', keyLength - Integer.toString(in).length())
				+ in;
		try { // 这里调用我们要测试的java类，这里我调用的是一个Test类
			sr.sampleStart(); // 记录程序执行时间，以及执行结果
			Object val = null;
			if (putOrGet.equals("put")) {
				VersionedValue<String> versionedValue=cache.getVersioned(key);
				if(versionedValue!=null){
					cache.replaceWithVersion(key, value, versionedValue.getVersion());
				}else{
					cache.put(key, value);
				}
				
				sr.setSuccessful(true);
			} else {
				val = cache.get(key);
				sr.setSuccessful(val != null);
			}
		} catch (Throwable e) {
			System.out.println("Exception is " + e.getMessage());
			sr.setSuccessful(false);
			sr.setErrorCount(1);
		} finally {
			sr.sampleEnd();
		}
		return sr;
	}

	public Arguments getDefaultParameters() {
		Arguments params = new Arguments();
		params.addArgument("cacheName", "");
		params.addArgument("size", "1024");
		params.addArgument("keyLength", "150");
		params.addArgument("keyRang", "100000");
		params.addArgument("putOrGet", "put");

		return params;

	}

}

package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TypedProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.util.Util;

public class TcpTransportFactory2 extends TcpTransportFactory {

	private static final Log log = LogFactory.getLog(
			TcpTransportFactory2.class, Log.class);

	/**
	 * We need synchronization as the thread that calls
	 * {@link org.infinispan.client.hotrod.impl.transport.TransportFactory#start(org.infinispan.client.hotrod.impl.protocol.Codec, org.infinispan.client.hotrod.impl.ConfigurationProperties, java.util.Collection, java.util.concurrent.atomic.AtomicInteger, ClassLoader)}
	 * might(and likely will) be different from the thread(s) that calls
	 * {@link #getTransport()} or other methods
	 */
	private final Object lock = new Object();
	// The connection pool implementation is assumed to be thread-safe, so we
	// need to synchronize just the access to this field and not the method
	// calls
	private GenericKeyedObjectPool connectionPool;
	private RequestBalancingStrategy balancer;
	private Collection<SocketAddress> servers;
	private ConsistentHash consistentHash;
	private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

	// the primitive fields are often accessed separately from the rest so it
	// makes sense not to require synchronization for them
	private volatile boolean tcpNoDelay;
	private volatile int soTimeout;
	private volatile int connectTimeout;
	private volatile int transportCount;
	private volatile int retryCount;

	@Override
	public void start(Codec codec, ConfigurationProperties cfg,
			Collection<SocketAddress> staticConfiguredServers,
			AtomicInteger topologyId, ClassLoader classLoader) {
		synchronized (lock) {
			hashFactory.init(cfg, classLoader);
			boolean pingOnStartup = cfg.getPingOnStartup();
			servers = Collections.unmodifiableCollection(new ArrayList(
					staticConfiguredServers));
			String balancerClass = cfg.getRequestBalancingStrategy();
			balancer = (RequestBalancingStrategy) Util.getInstance(
					balancerClass, classLoader);
			tcpNoDelay = cfg.getTcpNoDelay();
			soTimeout = cfg.getSoTimeout();
			connectTimeout = cfg.getConnectTimeout();
			retryCount = ((TypedProperties) cfg.getProperties())
					.getIntProperty("infinispan.client.hotrod.retry_count", 0);
			if (log.isInfoEnabled()) {
				log.infof("Statically configured servers: %s",
						staticConfiguredServers);
				log.infof(
						"retryCount = %d; Tcp no delay = %b; client socket timeout = %d ms; connect timeout = %d ms",
						retryCount, tcpNoDelay, soTimeout, connectTimeout);
			}
			PropsKeyedObjectPoolFactory poolFactory = new PropsKeyedObjectPoolFactory(
					new TransportObjectFactory(codec, this, topologyId,
							pingOnStartup), cfg.getProperties());
			createAndPreparePool(cfg, staticConfiguredServers, poolFactory);
			balancer.setServers(servers);
			updateTransportCount();
		}
	}

	/**
	 * This will makes sure that, when the evictor thread kicks in the minIdle
	 * is set. We don't want to do this is the caller's thread, as this is the
	 * user.
	 */
	private void createAndPreparePool(ConfigurationProperties cfg,
			Collection<SocketAddress> staticConfiguredServers,
			PropsKeyedObjectPoolFactory poolFactory) {
		connectionPool = (GenericKeyedObjectPool) poolFactory.createPool();
		String key = cfg.getProperties().getProperty(
				"infinispan.client.hotrod.prepare_pool", "false");
		boolean preparePool = Boolean.valueOf(key).equals(Boolean.TRUE);

		log.infof("infinispan.client.hotrod.prepare_pool = %b", preparePool);

		for (SocketAddress addr : staticConfiguredServers) {
			connectionPool.preparePool(addr, preparePool);
		}
	}

	@Override
	public void destroy() {
		synchronized (lock) {
			connectionPool.clear();
			try {
				connectionPool.close();
			} catch (Exception e) {
				log.warn("Exception while shutting down the connection pool.",
						e);
			}
		}
	}

	@Override
	public void updateHashFunction(
			Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners,
			short hashFunctionVersion, int hashSpace) {
		synchronized (lock) {
			ConsistentHash hash = hashFactory
					.newConsistentHash(hashFunctionVersion);
			if (hash == null) {
				log.noHasHFunctionConfigured(hashFunctionVersion);
			} else {
				hash.init(servers2Hash, numKeyOwners, hashSpace);
			}
			consistentHash = hash;
			log.infof("update consistent hash,consistentHash = %s",
					consistentHash);
		}
	}

	@Override
	public Transport getTransport() {
		SocketAddress server;
		synchronized (lock) {
			server = balancer.nextServer();
		}
		return borrowTransportFromPool(server);
	}

	@Override
	public Transport getTransport(byte[] key) {
		SocketAddress server;
		synchronized (lock) {
			if (consistentHash != null) {
				server = consistentHash.getServer(key);
				if (log.isTraceEnabled()) {
					log.tracef("Using consistent hash for determining the server: "
							+ server);
				}
			} else {
				server = balancer.nextServer();
				if (log.isTraceEnabled()) {
					log.tracef(
							"Using the balancer for determining the server: %s",
							server);
				}
			}
		}
		return borrowTransportFromPool(server);
	}

	@Override
	public void releaseTransport(Transport transport) {
		// The invalidateObject()/returnObject() calls could take a long time,
		// so we hold the lock only until we get the connection pool reference
		KeyedObjectPool pool = getConnectionPool();
		TcpTransport tcpTransport = (TcpTransport) transport;
		if (!tcpTransport.isValid()) {
			try {
				if (log.isTraceEnabled()) {
					log.tracef(
							"Dropping connection as it is no longer valid: %s",
							tcpTransport);
				}
				pool.invalidateObject(tcpTransport.getServerAddress(),
						tcpTransport);
			} catch (Exception e) {
				log.couldNoInvalidateConnection(tcpTransport, e);
				// log.warnf("Could not invalidate connection: %s",
				// e.toString());
			}
		} else {
			try {
				pool.returnObject(tcpTransport.getServerAddress(), tcpTransport);
			} catch (Exception e) {
				log.couldNotReleaseConnection(tcpTransport, e);
				// log.warnf("Could not release connection: %s", e.toString());
			} finally {
				logConnectionInfo(tcpTransport.getServerAddress());
			}
		}
	}

	@Override
	public void updateServers(Collection<SocketAddress> newServers) {
		synchronized (lock) {
			Set<SocketAddress> addedServers = new HashSet<SocketAddress>(
					newServers);
			addedServers.removeAll(servers);
			Set<SocketAddress> failedServers = new HashSet<SocketAddress>(
					servers);
			failedServers.removeAll(newServers);
			if (log.isTraceEnabled()) {
				log.tracef("Current list: %s", servers);
				log.tracef("New list: %s", newServers);
				log.tracef("Added servers: %s", addedServers);
				log.tracef("Removed servers: %s", failedServers);
			}
			if (failedServers.isEmpty() && newServers.isEmpty()) {
				log.debug("Same list of servers, not changing the pool");
				return;
			}

			// 1. first add new servers. For servers that went down, the
			// returned transport will fail for now
			for (SocketAddress server : addedServers) {
				log.newServerAdded(server);
				try {
					connectionPool.addObject(server);
				} catch (Exception e) {
					log.failedAddingNewServer(server, e);
					// log.warnf("Failed adding new server %s", e.toString());
				}
			}

			// 2. now set the server list to the active list of servers. All the
			// active servers (potentially together with some
			// failed servers) are in the pool now. But after this, the pool
			// won't be asked for connections to failed servers,
			// as the balancer will only know about the active servers
			balancer.setServers(newServers);

			// 3. Now just remove failed servers
			for (SocketAddress server : failedServers) {
				log.removingServer(server);
				connectionPool.clear(server);
			}

			servers = Collections.unmodifiableList(new ArrayList(newServers));
			updateTransportCount();
		}
	}

	public Collection<SocketAddress> getServers() {
		synchronized (lock) {
			return servers;
		}
	}

	private void logConnectionInfo(SocketAddress server) {
		if (log.isTraceEnabled()) {
			KeyedObjectPool pool = getConnectionPool();
			log.tracef("For server %s: active = %d; idle = %d", server,
					pool.getNumActive(server), pool.getNumIdle(server));
		}
	}

	private Transport borrowTransportFromPool(SocketAddress server) {
		// The borrowObject() call could take a long time, so we hold the lock
		// only until we get the connection pool reference
		KeyedObjectPool pool = getConnectionPool();
		try {
			return (Transport) pool.borrowObject(server);
		} catch (Exception e) {
			String message = "Could not fetch transport";
			log.couldNotFetchTransport(e);
			// log.warnf("Could not fetch transport:%s", e.toString());
			throw new TransportException(message, e);
		} finally {
			logConnectionInfo(server);
		}
	}

	/**
	 * Note that the returned <code>ConsistentHash</code> may not be
	 * thread-safe.
	 */
	public ConsistentHash getConsistentHash() {
		synchronized (lock) {
			return consistentHash;
		}
	}

	@Override
	public ConsistentHashFactory getConsistentHashFactory() {
		return hashFactory;
	}

	@Override
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	@Override
	public int getTransportCount() {
		if (Thread.currentThread().isInterrupted()) {
			return -1;
		}
		return transportCount;
	}

	@Override
	public int getSoTimeout() {
		return soTimeout;
	}

	@Override
	public int getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * Note that the returned <code>RequestBalancingStrategy</code> may not be
	 * thread-safe.
	 */
	public RequestBalancingStrategy getBalancer() {
		synchronized (lock) {
			return balancer;
		}
	}

	public GenericKeyedObjectPool getConnectionPool() {
		synchronized (lock) {
			return connectionPool;
		}
	}

	private void updateTransportCount() {
		synchronized (lock) {
			if (retryCount > 0) {
				transportCount = retryCount;
				return;
			}
			int maxActive = connectionPool.getMaxActive();
			if (maxActive > 0) {
				transportCount = Math
						.max(maxActive * servers.size(), maxActive);
			} else {
				transportCount = 10 * servers.size();
			}
		}
	}
}

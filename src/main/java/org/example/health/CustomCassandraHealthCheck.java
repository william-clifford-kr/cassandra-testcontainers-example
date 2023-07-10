package org.example.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Custom {@link HealthIndicator} for reporting on Cassandra connection status.
 * The spring-data-cassandra library adds a simple health indicator which shows
 * little more than up/down status. Here, we add some custom details that we
 * extract from the autoconfigured session.
 */
@Component("cassandra-health-check")
@ConditionalOnEnabledHealthIndicator("cassandra-health-check")
public class CustomCassandraHealthCheck implements HealthIndicator {

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomCassandraHealthCheck.class);

  private final CassandraProperties cassandraProperties;
  private final CqlSession cqlSession;

  @Autowired
  CustomCassandraHealthCheck(
    final CassandraProperties cassandraProperties
  ) {
    this.cassandraProperties = cassandraProperties;

    // We create the CqlSession instance here.
    // The other option is to have the ApplicationContext provide us one, but that winds
    // up being the primary (admin?) session. By creating our own, we do not use that
    // which is used by the application, keeping the health check session separate from
    // all other application-related operations.
    this.cqlSession = createCqlSession();
  }

  private CqlSession createCqlSession() {
    final List<InetSocketAddress> contactPoints = cassandraProperties.getContactPoints().stream()
      .map(hostname -> new InetSocketAddress(hostname, cassandraProperties.getPort()))
      .toList();

    final CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
      .addContactPoints(contactPoints)
      .withLocalDatacenter(cassandraProperties.getLocalDatacenter())
      .withKeyspace(cassandraProperties.getKeyspaceName());
    if (StringUtils.isNotBlank(cassandraProperties.getUsername())) {
      cqlSessionBuilder.withAuthCredentials(
        StringUtils.defaultString(cassandraProperties.getUsername()),
        StringUtils.defaultString(cassandraProperties.getPassword())
      );
    }

    return cqlSessionBuilder.build();
  }

  @Override
  public Health health() {
    final DriverContext context = cqlSession.getContext();
    final Metadata metadata = cqlSession.getMetadata();

    final Health.Builder builder = new Health.Builder()
      .withDetail("clusterName", metadata.getClusterName().orElse(""))
      .withDetail("driverExecutionProfiles",
        context.getConfig().getProfiles().keySet().stream().toList())
      .withDetail("hosts", getHosts(metadata))
      .withDetail("keySpaces", getKeySpaces(metadata))
      .withDetail("loadBalancingPolicies", getLoadBalancingPolicies(context))
      .withDetail("localDc", getConfiguredLogicalDataCenter(metadata));

    final NodesBiConsumer consumer = new NodesBiConsumer(new HashMap<>());
    metadata.getNodes().forEach(consumer);
    builder.withDetail("nodes", consumer.nodesMap());

    builder
      .withDetail("reconnectionPolicy", context.getReconnectionPolicy().getClass().getName())
      .withDetail("retryPolicies", getRetryPolicies(context))
      .withDetail("sessionName", context.getSessionName());

    try {
      cqlSession.execute("SELECT cluster_name FROM system.local;").forEach(row -> {
        LOGGER.debug("cluster_name: {}", row.getString(0));
      });
      builder.up();
    } catch (final Exception e) {
      builder.down(e);
    }

    return builder.build();
  }

  private static String getConfiguredLogicalDataCenter(@Nonnull final Metadata metadata) {
    return metadata.getNodes().values().stream()
      .findFirst()
      .map(Node::getDatacenter)
      .orElse("");
  }

  private static List<String> getHosts(@Nonnull final Metadata metadata) {
    return metadata.getNodes().values().stream()
      .filter(node -> node.getListenAddress().isPresent())
      .map(node -> node.getListenAddress().get().toString())
      .toList();
  }

  private static List<String> getKeySpaces(@Nonnull final Metadata metadata) {
    return metadata.getKeyspaces().keySet().stream()
      .map(identifier -> identifier.asCql(true))
      .toList();
  }

  private static List<String> getLoadBalancingPolicies(@Nonnull final DriverContext context) {
    return context.getLoadBalancingPolicies().keySet().stream().toList();
  }

  private static List<String> getRetryPolicies(@Nonnull final DriverContext context) {
    return context.getRetryPolicies().keySet().stream().toList();
  }

  record NodesBiConsumer(Map<UUID, Map<String, Object>> nodesMap) implements BiConsumer<UUID, Node> {
    @Override
    public void accept(@Nonnull final UUID uuid, @Nonnull final Node node) {
      final NodeDetailsConsumer consumer = new NodeDetailsConsumer(new TreeMap<>());
      consumer.accept(node);
      nodesMap.put(uuid, consumer.details());
    }
  }

  record NodeDetailsConsumer(Map<String, Object> details) implements Consumer<Node> {

    private static final InetSocketAddress ADDRESS_ANY = InetSocketAddress.createUnresolved("0.0.0.0", 0);

    @Override
    public void accept(@Nonnull final Node node) {
      details.clear();
      details.put("broadcastAddress", node.getBroadcastAddress().orElse(ADDRESS_ANY));
      details.put("broadcastRpcAddress", node.getBroadcastRpcAddress().orElse(ADDRESS_ANY));
      details.put("cassandraVersion", node.getCassandraVersion());
      details.put("datacenter", node.getDatacenter());
      details.put("distance", node.getDistance());
      details.put("endPoint", node.getEndPoint());
      details.put("extras", node.getExtras());
      details.put("hostId", node.getHostId());
      details.put("listenAddress", node.getListenAddress().orElse(ADDRESS_ANY));
      details.put("openConnections", node.getOpenConnections());
      details.put("rack", node.getRack());
      details.put("schemaVersion", node.getSchemaVersion());
      details.put("state", node.getState());
      details.put("upSinceMillis", convertUpSinceMillis(node.getUpSinceMillis()));
    }

    private static String convertUpSinceMillis(final long millis) {
      if (millis == -1) {
        return "";
      }
      return Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toString();
    }

  }

}

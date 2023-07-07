package org.example.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

@Component("cassandra-health-check")
@ConditionalOnEnabledHealthIndicator("cassandra-health-check")
public class CustomCassandraHealthCheck implements HealthIndicator {

  //    private final Cluster cluster;
  private final CqlSession cqlSession;
  private final Session session;

  @Autowired
  CustomCassandraHealthCheck(
    final CqlSession cqlSession,
    final Session session
  ) {
//        this.cluster = Cluster.builder()
//                .addContactPointsWithPorts(session.getMetadata().getNodes().values().stream()
//                        .map(Node::getBroadcastAddress)
//                        .filter(Optional::isPresent)
//                        .map(Optional::get)
//                        .toList())
//                .build();
    this.cqlSession = cqlSession;
    this.session = session;
  }

  private static final InetSocketAddress ADDRESS_ANY = InetSocketAddress.createUnresolved("0.0.0.0", 0);

  @Override
  public Health health() {
    final Health.Builder builder = new Health.Builder()
      .withDetail("clusterName", session.getMetadata().getClusterName().orElse(""))
      .withDetail("hosts", getHosts())
      .withDetail("loadBalancingPolicies",
        session.getContext().getLoadBalancingPolicies().keySet().stream().toList())
      .withDetail("localDc", getConfiguredLogicalDataCenter());

    final var keyspaces = new ArrayList<>(session.getMetadata().getKeyspaces().keySet().stream()
      .map(cqlIdentifier -> cqlIdentifier.asCql(true))
      .toList());
//        session.getMetadata().getKeyspaces().forEach((cqlIdentifier, keyspaceMetadata) -> {
//            keyspaces.add(Map.of(
//                    "cqlIdentifier", cqlIdentifier.asCql(true),
//                    "keyspaceMetadata", keyspaceMetadata.describeWithChildren(true)
//            ));
//        });
    builder.withDetail("keyspaces", keyspaces);

    final var nodes = new HashMap<>();
    session.getMetadata().getNodes().forEach((uuid, node) -> {
      final var nodeDetails = new HashMap<>(Map.of(
        "broadcastAddress", node.getBroadcastAddress().orElse(ADDRESS_ANY),
        "broadcastRpcAddress", node.getBroadcastRpcAddress().orElse(ADDRESS_ANY),
        "cassandraVersion", Objects.requireNonNull(node.getCassandraVersion()),
        "datacenter", Objects.requireNonNull(node.getDatacenter()),
        "distance", node.getDistance(),
        "endPoint", Objects.requireNonNull(node.getEndPoint()),
        "extras", node.getExtras(),
        "hostId", Objects.requireNonNull(node.getHostId()),
        "listenAddress", node.getListenAddress().orElse(ADDRESS_ANY),
        "openConnections", node.getOpenConnections()
      ));
      nodeDetails.putAll(Map.of(
        "rack", Objects.requireNonNull(node.getRack()),
        "schemaVersion", Objects.requireNonNull(node.getSchemaVersion()),
        "state", node.getState(),
        "upSinceMillis", node.getUpSinceMillis() == -1
          ? ""
          : Instant.ofEpochMilli(node.getUpSinceMillis()).atZone(ZoneId.systemDefault())
      ));
      nodes.put(uuid, nodeDetails);
    });
    builder.withDetail("nodes", nodes);

    try {
      cqlSession.execute("SELECT cluster_name FROM system.local;").forEach(row -> {
        System.out.printf("cluster_name: %s%n", row.getString(0));
      });
      builder.up();
    } catch (final Exception e) {
      builder.down(e);
    }

//        try (com.datastax.driver.core.Session s = cluster.connect()) {
//            s.execute("SELECT cluster_name FROM system.local;");
//            builder.up();
//        } catch (final Exception e) {
//            builder.down(e);
//        }

//        try (final CqlSession cqlSession = new CqlSessionBuilder()
//                .addContactPoints(session.getMetadata().getNodes().values().stream()
//                        .map(Node::getListenAddress)
//                        .filter(Optional::isPresent)
//                        .map(Optional::get)
//                        .toList())
//                .withLocalDatacenter(session.getMetadata().getNodes().values().stream()
//                        .map(Node::getDatacenter)
//                        .toList()
//                        .get(0))
//                .build()) {
//            cqlSession.execute("SELECT cluster_name FROM system.local;").all();
//            builder.up();
//        } catch (final Exception exception) {
//            builder.down(exception);
//        }

    return builder.build();
  }

  private String getConfiguredLogicalDataCenter() {
    return session.getMetadata().getNodes().values().stream()
      .findFirst()
      .map(Node::getDatacenter)
      .orElse("");
  }

  private List<String> getHosts() {
    return session.getMetadata().getNodes().values().stream()
      .filter(node -> node.getListenAddress().isPresent())
      .map(node -> node.getListenAddress().get().toString())
      .toList();
  }

}

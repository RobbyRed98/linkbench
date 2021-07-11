package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class LinkStoreDb2Graph extends LinkStoreDb2sql{

    public static final String CONFIG_NODE_LABEL = "nodelabel";
    public static final String CONFIG_LINK_LABEL = "linklabel";

    public static final String CONFIG_GRAPH_HOST = "graph_host";
    public static final String CONFIG_GRAPH_PORT = "graph_port";
    public static final String CONFIG_GRAPH_USER = "graph_user";
    public static final String CONFIG_GRAPH_PASSWORD = "graph_password";
    public static final String CONFIG_GRAPH_NAME = "graph_name";
    public static final String CONFIG_GRAPH_TRUST_STORE_PATH = "graph_truststore_path";
    public static final String CONFIG_GRAPH_TRUST_STORE_PASSWORD = "graph_truststore_password";
    public static final String CONFIG_GRAPH_SESSION_PREFIX = "graph_session_prefix";
    public static final String CONFIG_GRAPH_CONNECTION = "graph_connection";
    public static final String CONFIG_RANGE_LIMIT = "range_limit";


    String nodelabel;
    String linklabel;

    protected String graphHost = "";
    protected String graphUser = "";
    protected String graphPwd = "";
    protected int graphPort = 0;
    protected String graphName = "";
    protected String graphTrustStorePath = "";
    protected String graphTrustStorePwd = "";
    protected String graphSession = "";
    protected String graphConnection = "";
    protected String graphTravesalSourceName = "";
    protected GraphTraversalSource graphTraversalSource;
    protected Cluster graphCluster;
    protected Client graphClient;

    public LinkStoreDb2Graph() {
        super();
    }

    public LinkStoreDb2Graph(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);

        graphHost = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_HOST);
        graphPort = Integer.parseInt(ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PORT));
        graphUser = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_USER);
        graphPwd = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_PASSWORD);
        graphTrustStorePath = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_TRUST_STORE_PATH);
        graphTrustStorePwd = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_TRUST_STORE_PASSWORD);
        graphSession = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_SESSION_PREFIX) + threadId;
        graphConnection = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_CONNECTION);
        graphName = ConfigUtil.getPropertyRequired(props, CONFIG_GRAPH_NAME);
        graphTravesalSourceName = graphSession + "_" + graphConnection + "_" + graphName + "_traversal";

        try {
            rangeLimit = ConfigUtil.getInt(props, CONFIG_RANGE_LIMIT);
        } catch (LinkBenchConfigError ex) {
            logger.warn("Defaulting to " + DEFAULT_LIMIT + "as RANGE_LIMIT.");
            rangeLimit = DEFAULT_LIMIT;
        }

        nodelabel = ConfigUtil.getPropertyRequired(props, CONFIG_NODE_LABEL);
        linklabel = ConfigUtil.getPropertyRequired(props, CONFIG_LINK_LABEL);

        try {
            establishGraphConnection();
        } catch(Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Failed to connect to graph server");
        }
    }

    /**
     * Creates a connection to the db2 graph server.
     */
    protected void establishGraphConnection() {
        graphCluster = Cluster.build()
                .addContactPoint(graphHost)
                .credentials(graphUser, graphPwd)
                .trustStore(graphTrustStorePath)
                .trustStorePassword(graphTrustStorePwd)
                .enableSsl(true)
                .port(graphPort)
                .serializer(new GraphBinaryMessageSerializerV1())
                .create();

        graphClient = graphCluster.connect();


        Db2graphSessionState didSessionOpen = openSession(graphClient, graphSession, graphUser, graphPwd, logger);
        if (didSessionOpen == Db2graphSessionState.SUCCESS) {
            logger.info("Opened the new session '" + graphSession + "'.");
        } else {
            logger.info("The session '" + graphSession + "' already exists.");

            boolean didSessionClose;
            do {
                didSessionClose = closeSession(graphClient, graphSession, logger);
            } while (!didSessionClose);

            do {
                didSessionOpen = openSession(graphClient, graphSession, graphUser, graphPwd, logger);
            } while (didSessionOpen == Db2graphSessionState.FAILED);
            logger.info("Reopened session '" + graphSession + "'.");
        }

        boolean didConnectionOpen;
        do {
            didConnectionOpen = openConnection(graphClient, graphSession, graphConnection, user, pwd, logger);
            logger.trace("Opened graph connection for '" + graphSession + "'.");
        } while (!didConnectionOpen);

        graphTraversalSource = traversal().withRemote(DriverRemoteConnection.using(graphCluster, graphTravesalSourceName));

        // just a connection test, usually there are no Vertexes with TEST-Labels, thus it should return an empty list.
        graphTraversalSource.V().hasLabel("TEST").count().toList();
        logger.trace("Established connection to db2graph.");
    }

    @Override
    public void close() {
        super.close();
        try {
            graphTraversalSource.close();
            graphClient.closeAsync();
            graphCluster.closeAsync();
        } catch (Exception e) {
            logger.warn("Failed to close connection to the graph cluster.", e);
        }
    }

    protected Node getNodeGImpl(String dbid, int type, long id) throws ResponseException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type + " (graph)");

        Map<Object, Object> nodeId = createNodeId(dbid, nodelabel, id);

        List<Map<Object, Object>> nodeValueMaps = graphTraversalSource.V(nodeId)
                .hasLabel(nodelabel)
                .valueMap("ID", "TYPE", "VERSION", "TIME", "DATA")
                .by(unfold())
                .toList();

        if (nodeValueMaps.size() != 1) {
            return null;
        }

        Node node = valueMapToNode(nodeValueMaps.get(0));

        if (node.type != type) {
            logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + type);
            return null;
        }

        return node;
    }

    protected Link getLinkGImpl(String dbid, long id1, long link_type, long id2) throws ResponseException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type +
                    ", id2=" + id2 + " (graph)");
        }

        Map<Object, Object> linkId = createLinkId(dbid, linklabel, link_type, id1, id2);

        List<Map<Object, Object>> linkValueMaps = graphTraversalSource.E(linkId)
                .hasLabel(linklabel)
                .valueMap("ID1", "ID2", "LINK_TYPE", "VISIBILITY", "DATA", "TIME", "VERSION")
                .by(unfold())
                .toList();

        if (linkValueMaps.size() == 0) {
            logger.trace("getLink found no row");
            return null;
        } else if (linkValueMaps.size() > 1) {
            logger.warn("getNode id1=" + id1 + " id2=" + id2 + " link_type=" + link_type +
                    " returns the wrong amount of information: expected=1, actual=" + linkValueMaps.size());
            return null;
        }
        return valueMapToLink(linkValueMaps.get(0));
    }

    protected long countLinksGImpl(String dbid, long id1, long link_type) throws ResponseException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("countLinks for id1=" + id1 + " and link_type=" + link_type + " (graph)");

        var nodeId1 = createNodeId(dbid, nodelabel, id1);

        var countList = graphTraversalSource.V(nodeId1)
                .outE(linklabel)
                .count()
                .toList();

        if (countList.size() == 0) {
            logger.trace("countLinks found no row");
            return 0;
        } else if (countList.size() > 1) {
            logger.error("countLinks found more than one count for id1=" + id1 +
                    " and link_type=" + link_type + ": " + countList);
            throw new RuntimeException("Unexpected situation found more than one count for id1 link_type combination");
        }
        return countList.get(0);
    }

    protected Link[] multigetLinksGImpl(String dbid, long id1, long link_type, long[] id2s) throws ResponseException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("multigetLinks for id1=" + id1 + " and link_type=" + link_type + " and id2s " +
                    Arrays.toString(id2s) + " (graph)");

        var linkId = createLinkId(dbid, linklabel, link_type, id1, id2s[0]);

        List<Map<Object, Object>> linkValueMaps = graphTraversalSource.E(linkId)
                .valueMap("ID1", "ID2", "LINK_TYPE", "VISIBILITY", "DATA", "TIME", "VERSION")
                .by(unfold())
                .toList();

        Link[] links = new Link[linkValueMaps.size()];

        for (int i = 0; i < linkValueMaps.size(); i++) {
            links[i] = valueMapToLink(linkValueMaps.get(i));
        }

        if (links.length > 0) {
            if (Level.TRACE.isGreaterOrEqual(debuglevel))
                logger.trace("multigetLinks found " + links.length + " rows ");
            return links;
        } else {
            logger.trace("multigetLinks row not found");
            return new Link[0];
        }
    }

    protected Link[] getLinkListGImpl(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws ResponseException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLinkList for id1=" + id1 + ", link_type=" + link_type +
                    " minTS=" + minTimestamp + ", maxTS=" + maxTimestamp +
                    " offset=" + offset + ", limit=" + limit + " (graph)");
        }

        var nodeId1 = createNodeId(dbid, nodelabel, id1);

        List<Map<Object, Object>> linkValueMaps = graphTraversalSource.V(nodeId1)
                .outE(linklabel)
                .limit(limit)
                .valueMap("ID1", "ID2", "LINK_TYPE", "VISIBILITY", "DATA", "TIME", "VERSION")
                .by(unfold())
                .toList();

        Link[] links = new Link[linkValueMaps.size()];

        for (int i = 0; i < linkValueMaps.size(); i++) {
            links[i] = valueMapToLink(linkValueMaps.get(i));
        }

        if (links.length > 0) {
            if (Level.TRACE.isGreaterOrEqual(debuglevel))
                logger.trace("getLinkList found " + links.length + " rows ");
            return links;
        } else {
            logger.trace("getLinkList found no row");
            return null;
        }
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws ResponseException, SQLException, IOException {
        try {
            return getNodeGImpl(dbid, type, id);
        } catch (ResponseException ex) {
            processResponseException(ex, "countLinks");
            return getNodeGImpl(dbid, type, id);
        }
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws ResponseException, SQLException {
        try {
            return countLinksGImpl(dbid, id1, link_type);
        } catch (ResponseException ex) {
            processResponseException(ex, "countLinks");
            return countLinksGImpl(dbid, id1, link_type);
        }
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws ResponseException, SQLException, IOException {
        try {
            return getLinkGImpl(dbid, id1, link_type, id2);
        } catch (ResponseException ex) {
            processResponseException(ex, "getLink");
            return getLinkGImpl(dbid, id1, link_type, id2);
        }
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws ResponseException, SQLException, IOException {
        try {
            return multigetLinksGImpl(dbid, id1, link_type, id2s);
        } catch (ResponseException ex) {
            processResponseException(ex, "multigetLink");
            return multigetLinksGImpl(dbid, id1, link_type, id2s);
        }
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws ResponseException, SQLException, IOException {
        try {
            return getLinkListGImpl(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
        } catch (ResponseException ex) {
            processResponseException(ex, "getLinkList");
            return getLinkListGImpl(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
        }
    }

    private void processResponseException(ResponseException ex, String op) {
        logger.warn("ResponseException from " + op + ": " + ex);
        String msg = "ResponseException thrown by SQL driver during: " + op + ".  ";
        msg += "Message was: '" + ex.getMessage() + "'.  ";
        msg += "Error is transient, trying to reopen session.";
        logger.warn(msg);

        try {
            closeSession(graphClient, graphSession, logger);
            openSession(graphClient, graphSession, graphUser, graphPwd, logger);
        } catch(Exception exce) {
            openSession(graphClient, graphSession, graphUser, graphPwd, logger);
        }
    }

    private Link valueMapToLink(Map<Object, Object> valueMap) {
        Link link = new Link();
        link.id1 = (long) valueMap.get("ID1");
        link.id2 = (long) valueMap.get("ID2");
        link.link_type = (long) valueMap.get("LINK_TYPE");
        link.visibility = (byte) ((int) valueMap.get("VISIBILITY"));
        link.data = ((String) valueMap.get("DATA")).getBytes(StandardCharsets.US_ASCII);
        link.time = (long) valueMap.get("TIME");
        link.version = (int) ((long) valueMap.get("VERSION"));
        return link;
    }

    private Node valueMapToNode(Map<Object, Object> valueMap) {
        long id = (long) valueMap.get("ID");
        int type = (int) valueMap.get("TYPE");
        long version = ((BigDecimal) valueMap.get("VERSION")).longValue();
        int time = (int) valueMap.get("TIME");
        byte[] data = base64Decoder.decode((String) valueMap.get("DATA"));
        return new Node(id, type, version, time, data);
    }

    private Map<Object, Object> createNodeId(String dbid, String label, Long id) {
        return Map.of("prefix", String.format("%s.%s", dbid.toUpperCase(), label.toUpperCase()), "idCols", Collections.singletonList(id));
    }

    private Map<Object, Object> createLinkId(String dbid, String label, Long link_type,  Long id1, Long id2){
        return Map.of(
                "prefix", String.format("%s.%s", dbid.toUpperCase(), label.toUpperCase()),
                "idCols", Arrays.asList(link_type, id1, id2)
        );
    }

    @Override
    protected void addBulkCountsImpl(String dbid, List<LinkCount> counts) throws SQLException {
        logger.trace("Skipping adding a count because db2graph does not require a seperate table.");
    }

    @Override
    protected LinkWriteResult updateLinkImpl(String dbid, Link l, boolean noinverse) throws SQLException {
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("updateLink " + l.id1 + "." + l.id2 + "." + l.link_type);

        // Read and lock the row in Link
        int visibility = getVisibilityForUpdate(l.id1, l.link_type, l.id2, "updateLink");

        if (visibility == VISIBILITY_NOT_FOUND) {
            // Row doesn't exist
            logger.trace("updateLink row not found");
            conn_ac0.rollback();
            return LinkWriteResult.LINK_NOT_DONE;
        }

        // Update the row in Link
        pstmt_update_link_upd_link.setByte(1, l.visibility);
        setBytesAsVarchar(pstmt_update_link_upd_link, 2, l.data);
        pstmt_update_link_upd_link.setInt(3, l.version);
        pstmt_update_link_upd_link.setLong(4, l.time);
        pstmt_update_link_upd_link.setLong(5, l.id1);
        pstmt_update_link_upd_link.setLong(6, l.id2);
        pstmt_update_link_upd_link.setLong(7, l.link_type);

        int res = pstmt_update_link_upd_link.executeUpdate();
        if (res == 0) {
            logger.trace("updateLink row not changed");
            conn_ac0.rollback();
            return LinkWriteResult.LINK_NO_CHANGE;
        } else if (res != 1) {
            String s = "updateLink update failed with res=" + res +
                    " for id1=" + l.id1 + " id2=" + l.id2 + " link_type=" + l.link_type;
            logger.error(s);
            conn_ac0.rollback();
            throw new RuntimeException(s);
        }

        conn_ac0.commit();

        if (check_count)
            testCount(dbid, linktable, counttable, l.id1, l.link_type);

        return LinkWriteResult.LINK_UPDATE;
    }

    protected boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
                                     boolean noinverse, boolean expunge) throws SQLException {
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("deleteLink " + id1 + "." + id2 + "." + link_type);

        int visibility = getVisibilityForUpdate(id1, link_type, id2, "deleteLink");
        boolean found = (visibility != VISIBILITY_NOT_FOUND);

        if (!found || (visibility == VISIBILITY_HIDDEN && !expunge)) {
            logger.trace("deleteLinkImpl row not found");
            conn_ac0.rollback();
            return found;
        }

        // either delete or mark the link as hidden
        PreparedStatement wstmt;
        if (!expunge)
            wstmt = pstmt_delete_link_upd_link;
        else
            wstmt = pstmt_delete_link_del_link;

        wstmt.setLong(1, id1);
        wstmt.setLong(2, id2);
        wstmt.setLong(3, link_type);

        int update_res = wstmt.executeUpdate();
        if (update_res != 1) {
            String s = "deleteLink update link failed for id1=" + id1 +
                    " id2=" + id2 + " link_type=" + link_type;
            logger.error(s);
            conn_ac0.rollback();
            throw new RuntimeException(s);
        }

        conn_ac0.commit();

        if (check_count)
            testCount(dbid, linktable, counttable, id1, link_type);

        return found;
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws SQLException {
        checkNodeTableConfigured();
        // Truncate table deletes all data and allows us to reset autoincrement
        stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s IMMEDIATE;", dbid, linktable));
        stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s IMMEDIATE;", dbid, nodetable));
        // ALTER TABLE linkdb0.nodetable ALTER COLUMN id RESTART WITH 1
        stmt_ac1.execute(String.format("ALTER TABLE %s.%s ALTER COLUMN id " +
                "RESTART WITH 1;", dbid, nodetable, startID));
    }

    private boolean openConnection(Client graphClient, String graphSession,
                                   String graphConnection, String user,
                                   String pwd, Logger logger) {
        try {
            graphClient.submitAsync(getCommand("openConnection", graphSession, graphConnection, user, pwd))
                    .join().forEach(result -> logger.trace(result.getString()));
        } catch(CompletionException ex) {
            if (ex.getMessage().contains("java.util.ConcurrentModificationException")) {
                logger.trace("Opening connection in session '" + graphSession + "' failed due to a ConcurrentModification problem. Try again.");
                return false;
            }
        }
        return true;
    }

    private Db2graphSessionState openSession(Client graphClient, String graphSession,
                                             String graphUser, String graphPwd, Logger logger) {
        try {
            graphClient.submitAsync(getCommand("openSession", graphSession, graphUser, graphPwd)).join().forEach(result ->
                    logger.trace(result.getString())
            );
        } catch(CompletionException ex) {
            if (ex.getMessage().contains("already exists")) {
                logger.trace("Session '" + graphSession + "' already exists.");
                return Db2graphSessionState.SESSION_ALREADY_EXISTS;
            }

            if (ex.getMessage().contains("java.util.ConcurrentModificationException")) {
                logger.trace("Opening the session '" + graphSession + "' failed due to a ConcurrentModification problem. Try again.");
                return Db2graphSessionState.FAILED;
            }

            throw new RuntimeException("Unexpected situation in openSession. Only expected session 'already exists' or " +
                    "'ConcurrentModification' problem not: " + ex.getMessage());
        }
        return Db2graphSessionState.SUCCESS;
    }

    private boolean closeSession(Client graphClient, String graphSession, Logger logger) {
        try {
            graphClient.submitAsync(getCommand("closeSession", graphSession)).join().forEach(result ->
                    logger.trace(result.getString())
            );
        } catch(CompletionException ex) {
            if (ex.getMessage().contains("java.util.ConcurrentModificationException")) {
                logger.trace("Closing session '" + graphSession + "' failed due to a ConcurrentModification problem. Try again.");
                return false;
            }

            throw new RuntimeException("Unexpected situation in closeSession. Only expected session " +
                    "'ConcurrentModification' problem not: " + ex.getMessage());
        }
        return true;
    }

    private static String getCommand(String name, Object... params) {
        StringBuilder sb = new StringBuilder();
        sb.append("db2graph.");
        sb.append(name);
        sb.append("(");
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                Object p = params[i];
                if (i > 0) {
                    sb.append(",");
                }

                if (p instanceof String) {
                    sb.append("\"");
                    sb.append(p);
                    sb.append("\"");
                }
                else if (p instanceof Boolean) {
                    sb.append("\"");
                    sb.append((Boolean) p ? "yes" : "no");
                    sb.append("\"");
                }
                else {
                    sb.append(p);
                }
            }
        }
        sb.append(")");

        return sb.toString();
    }

    protected enum Db2graphSessionState {
        SUCCESS, SESSION_ALREADY_EXISTS, FAILED
    }
}

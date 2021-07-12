package com.facebook.LinkBench;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;

public class LinkStoreNeo4j extends GraphStore {

    /* database server configuration keys */
    public static final String CONFIG_HOST = "host";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_NODELABEL = "nodelabel";
    public static final String CONFIG_LINKLABEL = "linklabel";
    public static final String CONFIG_BULK_INSERT_COUNT = "bulk_insert_count";
    public static final String CONFIG_RANGE_LIMIT = "range_limit";

    Driver neo4jDriver;
    Session connection;

    // TODO consider making linklabel and nodelabel configurable

    String host;
    String port;

    protected Phase phase;

    protected String nodelabel;
    protected String linklabel;

    @Override
    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);
        try {
            host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
            port = ConfigUtil.getPropertyRequired(props, CONFIG_PORT);
            nodelabel = ConfigUtil.getPropertyRequired(props, CONFIG_NODELABEL);
            linklabel = ConfigUtil.getPropertyRequired(props, CONFIG_LINKLABEL);
            phase = currentPhase;

            try {
                rangeLimit = ConfigUtil.getInt(props, CONFIG_RANGE_LIMIT);
            } catch (LinkBenchConfigError ex) {
                logger.warn("Defaulting to " + DEFAULT_LIMIT + "as RANGE_LIMIT.");
                rangeLimit = DEFAULT_LIMIT;
            }

            try {
                bulkInsertCount = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_COUNT);
            } catch (LinkBenchConfigError ex) {
                logger.info("Defaulting to " + bulkInsertCount + "as BULK_INSERT_COUNT.");
            }

            openConnection();
        } catch (Exception ex) {
            String msg = "Failed to connect to neo4j: " + ex;
            logger.error(msg);
            throw new RuntimeException(msg);
        }
    }
    
    public void openConnection() throws Exception {
        neo4jDriver = GraphDatabase.driver("bolt://" + host + ":" + port, AuthTokens.none());
        connection = neo4jDriver.session();
    }

    @Override
    public void close() {
        neo4jDriver.close();
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        connection.writeTransaction(
                tx -> tx.run("MATCH (n:" + nodelabel + ") DETACH DELETE n")
        );
    }

    /* Node operations */

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        try {
            long[] ids = bulkAddNodesImpl(dbid, Collections.singletonList(node));
            if (ids.length != 1) {
                String msg = "addNode for " + node.id + " expected 1 returned " + ids.length;
                logger.error(msg);
                throw new RuntimeException(msg);
            }
            return ids[0];
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "addNode");
            throw ex;
        }
    }

    @Override
    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        try {
            return bulkAddNodesImpl(dbid, nodes);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "bulkAddNodes");
            throw ex;
        }
    }

    public long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("bulkAddNodes for " + nodes.size() + " nodes");
        }

        List<Long> results = connection.writeTransaction(tx ->
                nodes.stream().map(node ->
                        tx.run("CREATE (n:" + nodelabel + "{id: $id, type: $type, version: $version, time: $time, data: $data}) " +
                                        "RETURN n.id AS ID",
                                Map.of(
                                        "id", node.id,
                                        "type", node.type,
                                        "version", node.version,
                                        "time", node.time,
                                        "data", node.data
                                )
                        ).single()
                ).map(rec -> rec.get("ID").asLong()).collect(Collectors.toList())
        );

        if (results.size() != nodes.size()) {
            String msg = "bulkAddNodes insert for " + nodes.size() +
                    " returned " + results.size() + " with generated keys";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        long[] newIds = new long[results.size()];
        for (int i = 0; i < results.size(); i++) {
            newIds[i] = results.get(i);
        }
        return newIds;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        try {
            return getNodeImpl(dbid, type, id);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "getNode");
            throw ex;
        }
    }

    public Node getNodeImpl(String dbid, int type, long id) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type);

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n:" + nodelabel + "{id: $id, type: $type}) " +
                                "RETURN n.id AS ID, n.type AS TYPE, n.version AS VERSION, n.time AS TIME, n.data AS DATA",
                        Map.of(
                                "id", id,
                                "type", type
                        )
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("getNode found no node");
            return null;
        } else if (results.size() > 1) {
            logger.warn("getNode found " + results.size() + " nodes expected 1.");
            return null;
        }

        return recordToNode(results.get(0));
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        try {
            return updateNodeImpl(dbid, node);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "updateNode");
            throw ex;
        }
    }

    public boolean updateNodeImpl(String dbid, Node node) throws Neo4jException {
        long updatedNodeCount = connection.writeTransaction(tx ->
                tx.run("MATCH (n:" + nodelabel + "{id: $id}) SET n.type = $type, n.version = $version, n.time = $time, n.data = $data " +
                                "RETURN COUNT(n) AS COUNT",
                        Map.of(
                                "id", node.id,
                                "type", node.type,
                                "version", node.version,
                                "time", node.time,
                                "data", node.data
                        )).single().get("COUNT").asLong()
        );

        if (updatedNodeCount == 0) {
            return false;
        } else if (updatedNodeCount != 1) {
            String msg = "Updated multiple links but should have one.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return true;
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        try {
            return deleteNodeImpl(dbid, type, id);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "deleteNode");
            throw ex;
        }
    }

    public boolean deleteNodeImpl(String dbid, int type, long id) throws Neo4jException {
        long deletedNodeCount = connection.writeTransaction(tx ->
            tx.run("MATCH (n:" + nodelabel + "{id: $id}) WITH n, COUNT(n.id) AS COUNT " +
                            "DETACH DELETE n RETURN COUNT",
                    Map.of("id", id)
            ).single().get("COUNT").asLong()
        );

        if (deletedNodeCount == 0) {
            return false;
        } else if (deletedNodeCount != 1) {
            String msg = "Deleted multiple nodes instead of one.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return true;
    }

    /* Link operations */

    @Override
    public LinkWriteResult addLink(String dbid, Link link, boolean noinverse) throws Exception {
        return newLinkLoop(dbid, link, noinverse, true, "addLink");
    }

    public void addLinkImpl(String dbid, Link link, boolean noinverse) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addLink enter for " + link.id1 + "." + link.id2 + "." + link.link_type);

        long createdLinkCount = connection.writeTransaction(tx -> tx.run(
                        "MATCH (n1:" + nodelabel + "{id:$id1}), (n2:" + nodelabel + "{id:$id2}) CREATE (n1)-" +
                                "[l:" + linklabel + "{link_type: $link_type, visibility: $visibility, " +
                                "data: $data, time: $time, version: $version}]" +
                                "->(n2) RETURN COUNT(l) AS COUNT",
                        Map.of(
                                "id1", link.id1,
                                "id2", link.id2,
                                "link_type", link.link_type,
                                "visibility", link.visibility,
                                "data", link.data,
                                "time", link.time,
                                "version", link.version
                        )).single()
                ).get("COUNT").asLong();

        if (createdLinkCount != 1) {
            String msg = "Operation expected to add 1 link added " + createdLinkCount + " links.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
    }

    @Override
    public void addBulkLinks(String dbid, List<Link> a, boolean noinverse) throws Exception {
        try {
            addBulkLinksImpl(dbid, a, false);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "addBulkLinks");
            throw ex;
        }
    }

    public void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addBulkLinks: " + links.size() + " links");

        long createdLinkCount = connection.writeTransaction(tx ->
                links.stream().map(l -> tx.run(
                        "MATCH (n1:" + nodelabel + "{id:$id1}), (n2:" + nodelabel + "{id:$id2}) CREATE (n1)-" +
                                "[l:" + linklabel + "{link_type: $link_type, visibility: $visibility, " +
                                "data: $data, time: $time, version: $version}]" +
                                "->(n2) RETURN COUNT(l) AS COUNT",
                        Map.of(
                                "id1", l.id1,
                                "id2", l.id2,
                                "link_type", l.link_type,
                                "visibility", l.visibility,
                                "data", l.data,
                                "time", l.time,
                                "version", l.version
                        )).single()
                ).mapToLong(rec -> rec.get("COUNT").asLong()).sum()
        );

        if (createdLinkCount != links.size()) {
            String msg = "addBulkLinks insert of " + links.size() + " links" +
                    " returned " + createdLinkCount;
            logger.trace(msg);
            logger.trace("That can happen if a too small maxID in combination the real distribution was chosen.");
        }
    }

    @Override
    public LinkWriteResult updateLink(String dbid, Link link, boolean noinverse) throws Exception {
        return newLinkLoop(dbid, link, noinverse, false, "updateLink");
    }

    public LinkWriteResult updateLinkImpl(String dbid, Link link, boolean noinverse) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("updateLink " + link.id1 + "." + link.id2 + "." + link.link_type);

        long updateLinkCount = connection.writeTransaction(tx ->
                tx.run(
                        "MATCH (n1:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}]->(n2:" + nodelabel + "{id: $id2}) SET " +
                                "l.visibility = $visibility, " +
                                "l.data = $data, " +
                                "l.time = $time, " +
                                "l.version = $version " +
                                "RETURN COUNT(l) AS COUNT",
                        Map.of(
                                "id1", link.id1,
                                "id2", link.id2,
                                "link_type", link.link_type,
                                "visibility", link.visibility,
                                "data", link.data,
                                "time", link.time,
                                "version", link.version
                        )
                ).single().get("COUNT").asLong());

        if (updateLinkCount == 0) {
            return LinkWriteResult.LINK_NOT_DONE;
        } else if (updateLinkCount != 1) {
            String msg = "Updated multiple links but should have updated only one.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return LinkWriteResult.LINK_UPDATE;
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        try {
            return deleteLinkImpl(dbid, id1, link_type, id2);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "deleteLink");
            throw ex;
        }
    }

    public boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("deleteLink " + id1 + "." + id2 + "." + link_type);
        
        long deletedLinkCount = connection.writeTransaction(tx ->
                tx.run(
                        "MATCH (:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}}]->(:" + nodelabel + "{id: $id2}) " +
                                "WITH l, COUNT(l) AS COUNT DELETE l RETURN COUNT",
                        Map.of(
                                "id1", id1,
                                "id2", id2,
                                "link_type", link_type
                        )
                ).single().get("COUNT").asLong());

        if (deletedLinkCount == 0) {
            return false;
        } else if (deletedLinkCount != 1) {
            String msg = "Deleted multiple links but should have one.";
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return true;
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        try {
            return getLinkImpl(dbid, id1, link_type, id2);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "getLink");
            throw ex;
        }
    }

    public Link getLinkImpl(String dbid, long id1, long id2, long link_type) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type + ", id2=" + id2);
        }

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n1:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}]->(n2:" + nodelabel + "{id: $id2}) RETURN " +
                                "n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                                "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION",
                        Map.of(
                                "id1", id1,
                                "id2", id2,
                                "link_type", link_type
                        )
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("getLink found no link");
            return null;
        } else if (results.size() > 1) {
            logger.warn("getLink found " + results.size() + "links expected 1.");
            return null;
        }

        return recordToLink(results.get(0));
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws Exception {
        try {
            return multigetLinksImpl(id1, link_type, id2s);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "multigetLinks");
            throw ex;
        }
    }

    public Link[] multigetLinksImpl(long id1, long link_type, long[] id2s) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("multigetLinks for id1=" + id1 + " and link_type=" + link_type + " and id2s " +
                    Arrays.toString(id2s));
        }

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n1:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}]->(n2:" + nodelabel + "{id: $id2}) " +
                                "RETURN " +
                                "n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                                "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION",
                        Map.of(
                                "id1", id1,
                                "id2", id2s[0],
                                "link_type", link_type
                        )
                ).list()
        );

        if (results.size() < 1) {
            logger.trace("multigetLink found no link.");
            return null;
        }

        logger.trace("multigetlink found " + results.size() + " links.");

        Link[] links = new Link[results.size()];
        for (int i = 0; i < results.size(); i++) {
            links[i] = recordToLink(results.get(i));
        }

        return links;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        try {
            return getLinkListImpl(dbid, id1, link_type, limit);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "getLinkList");
            throw ex;
        }
    }

    public Link[] getLinkListImpl(String dbid, long id1, long link_type, int limit) {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLinkList for id1=" + id1 + ", link_type=" + link_type + ", limit=" + limit);
        }

        List<Record> results = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (n1:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}]->(n2:" + nodelabel + ") RETURN " +
                                "n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                                "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION LIMIT $limit",
                        Map.of(
                                "id1", id1,
                                "link_type", link_type,
                                "limit", limit
                        )
                ).list()
        );

        Link[] links = new Link[results.size()];
        for (int i = 0; i < results.size(); i++) {
            links[i] = recordToLink(results.get(i));
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
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        try {
            return countLinksImpl(id1, link_type);
        } catch (Neo4jException ex) {
            processNeo4jException(ex, "multigetLinks");
            throw ex;
        }
    }

    public long countLinksImpl(long id1, long link_type) throws Neo4jException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("countLinks for id= " + id1 + " link_type=" + link_type);

        long linkCount = connection.readTransaction(tx ->
                tx.run(
                        "MATCH (:" + nodelabel + "{id: $id1})-[l:" + linklabel + "{link_type: $link_type}]->(:" + nodelabel + ") RETURN COUNT(l) AS COUNT",
                        Map.of(
                                "id1", id1,
                                "link_type", link_type
                        )
                ).single().get("COUNT").asLong()
        );

        if (linkCount == 0) {
            logger.trace("countLinks found no row");
            return 0;
        }

        return linkCount;
    }

    protected LinkWriteResult newLinkLoop(String dbid, Link link, boolean noinverse,
                                          boolean insert_first, String caller) throws SQLException {
        boolean do_insert = insert_first;
        while (true) {
            if (do_insert) {
                addLinkImpl(dbid, link, noinverse);
                return LinkWriteResult.LINK_INSERT;
            } else {
                LinkWriteResult wr = updateLinkImpl(dbid, link, noinverse);
                if (wr == LinkWriteResult.LINK_UPDATE) {
                  return wr;
                } else if (wr == LinkWriteResult.LINK_NOT_DONE) {
                    // Row does not exist, switch to insert
                    do_insert = true;
                    retry_upd_to_add.incrementAndGet();
                    logger.debug("newLinkLoop upd_to_add for id1=" + link.id1 + " id2=" + link.id2 +
                            " link_type=" + link.link_type + " gap " + (link.id2 - link.id1));
                } else {
                    String s = "newLinkLoop bad result for update(" + wr + ") with id1=" + link.id1 +
                            " id2=" + link.id2 + " link_type=" + link.link_type;
                    logger.error(s);
                    throw new RuntimeException(s);
                }
            }
        }
    }

    @Override
    public void addBulkCounts(String dbid, List<LinkCount> counts) throws SQLException {
        logger.trace("Is ignored because neo4j does not create counts.");
    }

    @Override
    public void clearErrors(int threadID) {
            logger.info("Reopening Neo4j connection in threadID " + threadID);
            try {
                connection.close();
                openConnection();
            } catch (Throwable e) {
                e.printStackTrace();
            }
    }

    protected static Node recordToNode(Record record) {
        long id = record.get("ID").asLong();
        int type = record.get("TYPE").asInt();
        long version = record.get("VERSION").asLong();
        int time = record.get("TIME").asInt();
        byte[] data = record.get("DATA").asByteArray();
        return new Node(id, type, version, time, data);
    }

    protected static Link recordToLink(Record record) {
        Link link = new Link();
        link.id1 = record.get("ID1").asLong();
        link.id2 = record.get("ID2").asLong();
        link.link_type = record.get("LINK_TYPE").asLong();
        link.visibility = record.get("VISIBILITY").asNumber().byteValue();
        link.version = record.get("VERSION").asInt();
        link.time = record.get("TIME").asLong();
        link.data = record.get("DATA").asByteArray();
        return link;
    }

    protected void processNeo4jException(Neo4jException ex, String op) {
        String msg = "SQLException thrown by SQL driver during: " + op + ".  ";
        msg += "Message was: '" + ex.getMessage() + "'.  ";
        msg += "Neo4j-code was: " + ex.code() + ".  ";
        logger.error(msg);
    }
}

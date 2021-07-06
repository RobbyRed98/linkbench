package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LinkStoreDb2GraphOldCypher extends LinkStoreDb2GraphOld {


    private Client graphClient;

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);
        graphClient = graphCluster.connect();
    }

    protected Node getNodeGImpl(String dbid, int type, long id) throws ResponseException, ExecutionException, InterruptedException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type + " (graph)");

        String cypherQuery = "MATCH (n:" + nodelabel + "{id: " + id + "}) " +
                "RETURN n.id AS ID, n.type AS TYPE, n.version AS VERSION, n.time AS TIME, n.data AS DATA";

        RequestMessage cypherRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        List<LinkedHashMap> resultValues = graphClient.submitAsync(cypherRequest)
                .get()
                .stream()
                .map(r -> r.get(LinkedHashMap.class))
                .collect(Collectors.toList());

        if (resultValues.size() != 1) {
            return null;
        }

        Node node = valueMapToNode(resultValues.get(0));

        if (node.type != type) {
            logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + type + ")");
            return null;
        }

        return node;
    }

    protected Link getLinkGImpl(String dbid, long id1, long link_type, long id2) throws ResponseException, ExecutionException, InterruptedException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type + ", id2=" + id2 + " (graph)");

        String cypherQuery =
                "MATCH (n1:" + nodelabel + "{id: " + id1 + "})-[l:" + linklabel + "{link_type: " + link_type + "}]->(n2:" + nodelabel + "{id: " + id2 + "}) " +
                        "RETURN n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                        "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION";

        RequestMessage cypherRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        List<LinkedHashMap> resultValues = graphClient.submitAsync(cypherRequest)
                .get()
                .stream()
                .map(r -> r.get(LinkedHashMap.class))
                .collect(Collectors.toList());

        if (resultValues.size() == 0) {
            logger.trace("getLink found no row");
            return null;
        } else if (resultValues.size() > 1) {
            logger.warn("getNode id1=" + id1 + " id2=" + id2 + " link_type=" + link_type +
                    " returns the wrong amount of information: expected=1, actual=" + resultValues.size());
            return null;
        }
        return valueMapToLink(resultValues.get(0));
    }

    protected long countLinksGImpl(String dbid, long id1, long link_type) throws ResponseException, ExecutionException, InterruptedException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("countLinks for id1=" + id1 + " and link_type=" + link_type + " (graph)");

        String cypherQuery = "MATCH (:" + nodelabel + "{id: " + id1 + "})-[l:" + linklabel + "{link_type: " + link_type +
                "}]->(:" + linklabel + ") RETURN COUNT(l)";

        RequestMessage cypherRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        List<Long> countList = graphClient.submitAsync(cypherRequest)
                .get()
                .stream()
                .map(r -> r.get(LinkedHashMap.class))
                .filter(lhm -> lhm.containsKey("COUNT(l)"))
                .map(lhm -> (Long) lhm.get("COUNT(l)"))
                .collect(Collectors.toList());

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

    protected Link[] multigetLinksGImpl(String dbid, long id1, long link_type, long[] id2s) throws ResponseException, ExecutionException, InterruptedException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("multigetLinks for id1=" + id1 + " and link_type=" + link_type + " and id2s " +
                    Arrays.toString(id2s) + " (graph)");

        String cypherQuery =
                "MATCH (n1:" + nodelabel + "{id: " + id1 + "})-[l:" + linklabel + "{link_type: " + link_type + "}]->(n2:" + nodelabel + ") " +
                        "WHERE n2.id IN " + Arrays.toString(id2s) + " " +
                        "RETURN n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                        "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION";

        RequestMessage cypherRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        List<LinkedHashMap> resultValues = graphClient.submitAsync(cypherRequest)
                .get()
                .stream()
                .map(r -> r.get(LinkedHashMap.class))
                .collect(Collectors.toList());


        Link[] links = new Link[resultValues.size()];

        for (int i = 0; i < resultValues.size(); i++) {
            links[i] = valueMapToLink(resultValues.get(i));
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

    protected Link[] getLinkListGImpl(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws ResponseException, ExecutionException, InterruptedException {
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
            logger.trace("getLinkList for id1=" + id1 + ", link_type=" + link_type +
                    " minTS=" + minTimestamp + ", maxTS=" + maxTimestamp +
                    " offset=" + offset + ", limit=" + limit + " (graph)");
        }

        String cypherQuery =
                "MATCH (n1:" + nodelabel + "{id: " + id1 + "})-[l:" + linklabel + "{link_type: " + link_type + "}]->(n2:" + nodelabel + ") " +
                        "RETURN n1.id AS ID1, n2.id AS ID2, l.link_type AS LINK_TYPE, " +
                        "l.visibility AS VISIBILITY, l.data AS DATA, l.time AS TIME, l.version AS VERSION LIMIT " + limit;

        RequestMessage cypherRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("cypher")
                .add(Tokens.ARGS_GREMLIN, cypherQuery)
                .create();

        List<LinkedHashMap> resultValues = graphClient.submitAsync(cypherRequest)
                .get()
                .stream()
                .map(r -> r.get(LinkedHashMap.class))
                .collect(Collectors.toList());

        Link[] links = new Link[resultValues.size()];

        for (int i = 0; i < resultValues.size(); i++) {
            links[i] = valueMapToLink(resultValues.get(i));
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
    public Node getNode(String dbid, int type, long id) throws ResponseException, SQLException, IOException, ExecutionException, InterruptedException {
        return getNodeGImpl(dbid, type, id);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws ResponseException, SQLException, ExecutionException, InterruptedException {
        return countLinksGImpl(dbid, id1, link_type);
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws ResponseException, SQLException, IOException, ExecutionException, InterruptedException {
        return getLinkGImpl(dbid, id1, link_type, id2);
    }

    @Override
    public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws ResponseException, SQLException, IOException, ExecutionException, InterruptedException {
        return multigetLinksGImpl(dbid, id1, link_type, id2s);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws ResponseException, SQLException, IOException, ExecutionException, InterruptedException {
        return getLinkListGImpl(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }
}

/* * LinkStore for MySQL
 * Author: Mark Callaghan
 * Date : Feb 2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench;

import javax.sql.rowset.serial.SerialClob;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class LinkStoreDb2sql extends LinkStoreSql {

    public Base64.Encoder base64Encoder = Base64.getEncoder();
    public Base64.Decoder base64Decoder = Base64.getDecoder();
    private int maxlength = 0;

    public LinkStoreDb2sql() {
        super();
    }

    public LinkStoreDb2sql(Properties props) throws IOException, Exception {
        super();
        initialize(props, Phase.LOAD, 0);
    }

    public void initialize(Properties props, Phase currentPhase, int threadId) {
        super.initialize(props, currentPhase, threadId);
    }

    protected PreparedStatement makeAddLinkIncCountPS() throws SQLException {
        String sql = "MERGE INTO " + init_dbid + "." + counttable + " as current" +
                " USING ( VALUES (?, ?, ?, ?, 0) ) as new (id, link_type, count, time, version)" +
                " ON current.id = new.id AND current.link_type = new.link_type" +
                " WHEN MATCHED THEN" +
                " UPDATE SET current.count = current.count + ?," +
                " current.version = current.version + 1," +
                " current.time = ?" +
                " WHEN NOT MATCHED THEN" +
                " INSERT (id, link_type, count, time, version)" +
                " VALUES (new.id, new.link_type, new.count, new.time, new.version)";

        logger.debug("addLinkIncCount PS: " + sql);
        return conn_ac0.prepareStatement(sql);
    }

    protected PreparedStatement makeGetLinkListPS() throws SQLException {
        String sql = "SELECT id1, id2, link_type," +
                " visibility, data, version, time" +
                " FROM " + init_dbid + "." + linktable +
                " WHERE id1 = ? AND link_type = ? " +
                " AND time >= ?" +
                " AND time <= ?" +
                " AND visibility = " + LinkStore.VISIBILITY_DEFAULT +
                " ORDER BY time DESC" +
                " LIMIT ? OFFSET ?";
        logger.debug("getLinkList PS: " + sql);
        return conn_ac1.prepareStatement(sql);
    }

    // This hardwires Linkbench to use the database "linkbench"
    protected String getJdbcUrl() {
        return "jdbc:db2://" + host + ":" + port + "/linkdb0";
    }

    protected String getJdbcClassName() {
        return "com.ibm.db2.jcc.DB2Driver";
    }

    protected String getJdbcOptions() {
        // It isn't clear that these make a big difference
        return "";
    }

    /**
     * Set of all JDBC SQLState strings that indicate a transient error
     * that should be handled by retrying
     */
    protected HashSet<String> populateRetrySQLStates() {
        // TODO are there more?
        HashSet<String> states = new HashSet<String>();
        states.add("41000"); // ER_LOCK_WAIT_TIMEOUT
        states.add("40001"); // ER_LOCK_DEADLOCK
        return states;
    }

    protected boolean isDupKeyError(SQLException ex) {
        logger.trace("isDupKeyError for : " + ex +
                " with state " + ex.getSQLState());
        return ex.getSQLState().equals("23505");
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws SQLException {
        checkNodeTableConfigured();
        // Truncate table deletes all data and allows us to reset autoincrement
        stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s IMMEDIATE;", dbid, linktable));
        stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s IMMEDIATE;", dbid, counttable));
        stmt_ac1.execute(String.format("TRUNCATE TABLE %s.%s IMMEDIATE;", dbid, nodetable));
        // ALTER TABLE linkdb0.nodetable ALTER COLUMN id RESTART WITH 1
        stmt_ac1.execute(String.format("ALTER TABLE %s.%s ALTER COLUMN id " +
                "RESTART WITH 1;", dbid, nodetable, startID));
    }

    String getDefaultPort() {
        return "50000";
    }

    protected void addLinkChangeCount(String dbid, Link l, int base_count, PreparedStatement pstmt)
            throws SQLException {

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addLink change count");

        long now = (new Date()).getTime();
        pstmt.setLong(1, l.id1);
        pstmt.setLong(2, l.link_type);
        pstmt.setLong(3, base_count);
        pstmt.setLong(4, now);
        pstmt.setLong(5, base_count);
        pstmt.setLong(6, now);

        int update_res = pstmt.executeUpdate();
        // 1 means insert, 2 means update, 0 means no change, other values are not defined
        if (update_res != 1 && update_res != 2) {
            String e = "addLink increment count failed with res=" +
                    update_res + " for id1=" + l.id1 +
                    " id2=" + l.id2 + " link_type=" + l.link_type;
            logger.error(e);
            conn_ac0.rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    protected long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws SQLException {
        checkNodeTableConfigured();
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("bulkAddNodes for " + nodes.size() + " nodes");

        PreparedStatement pstmt;
        boolean must_close_pstmt = false;

        if (nodes.size() == 1)
            pstmt = pstmt_add_node_1;
        else if (nodes.size() == bulkLoadBatchSize())
            pstmt = pstmt_add_node_n;
        else {
            pstmt = makeAddNodePS(nodes.size());
            must_close_pstmt = true;
        }

        int x = 1;
        for (Node node: nodes) {
            pstmt.setInt(x, node.type);
            pstmt.setLong(x+1, node.version);
            pstmt.setInt(x+2, node.time);
            setBytesAsClob(pstmt, x+3, node.data);
            x += 4;
        }

        int res = pstmt.executeUpdate();
        ResultSet rs = pstmt.getGeneratedKeys();

        long newIds[] = new long[nodes.size()];
        // Find the generated id
        int i = 0;
        while (rs.next() && i < nodes.size()) {
            newIds[i++] = rs.getLong(1);
        }

        if (res != nodes.size() || i != nodes.size()) {
            String s = "bulkAddNodes insert for " + nodes.size() +
                    " returned " + res + " with " + i + " generated keys ";
            logger.error(s);
            throw new RuntimeException(s);
        }

        assert(!rs.next()); // check done
        rs.close();
        if (must_close_pstmt)
            pstmt.close();
        return newIds;
    }

    protected boolean updateNodeImpl(String dbid, Node node) throws SQLException {
        checkNodeTableConfigured();
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("updateNode for id " + node.id);

        pstmt_update_node.setLong(1, node.version);
        pstmt_update_node.setInt(2, node.time);
        setBytesAsClob(pstmt_update_node, 3, node.data);
        pstmt_update_node.setLong(4, node.id);
        pstmt_update_node.setInt(5, node.type);

        int rows = pstmt_update_node.executeUpdate();

        if (rows == 1)
            return true;
        else if (rows == 0)
            return false;
        else {
            String s = "updateNode expected 1 or 0 but returned " + rows +
                    " for id=" + node.id;
            logger.error(s);
            throw new RuntimeException(s);
        }
    }

    protected Node getNodeImpl(String dbid, int type, long id) throws SQLException, IOException {
        checkNodeTableConfigured();
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("getNode for id= " + id + " type=" + type);

        pstmt_get_node.setLong(1, id);

        ResultSet rs = pstmt_get_node.executeQuery();

        if (rs.next()) {
            Node res = new Node(rs.getLong(1), rs.getInt(2), rs.getLong(3), rs.getInt(4), getClobAsBytes(rs, 5));

            // Check that multiple rows weren't returned
            assert(rs.next() == false);
            rs.close();

            if (res.type != type) {
                logger.warn("getNode found id=" + id + " with wrong type (" + type + " vs " + res.type);
                return null;
            } else {
                return res;
            }

        } else {
            rs.close();
            return null;
        }
    }

    private void setBytesAsClob(PreparedStatement pstmt, int i, byte[] bytes) throws SQLException {
        pstmt.setClob(i, new SerialClob(base64Encoder.encodeToString(bytes).toCharArray()));
    }

    private byte[] getClobAsBytes(ResultSet rs, int i) throws SQLException, IOException {
        return base64Decoder.decode(rs.getClob(i).getAsciiStream().readAllBytes());
    }

    protected void addLinkImpl(String dbid, Link l, boolean noinverse) throws SQLException {
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addLink enter for " + l.id1 + "." + l.id2 + "." + l.link_type);

        pstmt_add_link_ins_link.setLong(1, l.id1);
        pstmt_add_link_ins_link.setLong(2, l.id2);
        pstmt_add_link_ins_link.setLong(3, l.link_type);
        pstmt_add_link_ins_link.setByte(4, l.visibility);
        setBytesAsVarchar(pstmt_add_link_ins_link, 5, l.data);
        pstmt_add_link_ins_link.setInt(6, l.version);
        pstmt_add_link_ins_link.setLong(7, l.time);

        // If the link is there then the caller switches to updateLinkImpl
        int ins_result = pstmt_add_link_ins_link.executeUpdate();

        int base_count = 1;
        if (l.visibility != VISIBILITY_DEFAULT)
            base_count = 0;

        addLinkChangeCount(dbid, l, base_count, pstmt_add_link_inc_count);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addLink commit with count change");

        conn_ac0.commit();

        if (check_count)
            testCount(dbid, linktable, counttable, l.id1, l.link_type);
    }

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

        // If needed, increment or decrement Count
        if (visibility != l.visibility) {
            int update_count;

            if (l.visibility == VISIBILITY_DEFAULT)
                update_count = 1;
            else
                update_count = -1;

            pstmt_link_inc_count.setInt(1, update_count);
            pstmt_link_inc_count.setLong(2, (new Date()).getTime());
            pstmt_link_inc_count.setLong(3, l.id1);
            pstmt_link_inc_count.setLong(4, l.link_type);

            int update_res = pstmt_link_inc_count.executeUpdate();
            if (update_res != 1) {
                String s = "updateLink increment count failed with res=" + res +
                        " for id1=" + l.id1 + " link_type=" + l.link_type;
                logger.error(s);
                conn_ac0.rollback();
                throw new RuntimeException(s);
            }
        }

        conn_ac0.commit();

        if (check_count)
            testCount(dbid, linktable, counttable, l.id1, l.link_type);

        return LinkWriteResult.LINK_UPDATE;
    }

    protected Link createLinkFromRow(ResultSet rs) throws SQLException, IOException {
        Link l = new Link();
        l.id1 = rs.getLong(1);
        l.id2 = rs.getLong(2);
        l.link_type = rs.getLong(3);
        l.visibility = rs.getByte(4);
        l.data = getVarcharAsBytes(rs, 5);
        l.version = rs.getInt(6);
        l.time = rs.getLong(7);
        return l;
    }

    protected void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse) throws SQLException {
        checkDbid(dbid);

        if (Level.TRACE.isGreaterOrEqual(debuglevel))
            logger.trace("addBulkLinks: " + links.size() + " links");

        if (!noinverse) {
            String s = "addBulkLinks does not handle inverses";
            logger.error(s);
            throw new RuntimeException(s);
        }

        PreparedStatement pstmt;
        boolean must_close_pstmt = false;

        if (links.size() == bulkLoadBatchSize())
            pstmt = pstmt_add_bulk_links_n;
        else {
            pstmt = makeAddLinksPS(links.size(), true);
            must_close_pstmt = true;
        }

        int x = 1;
        for (Link link: links) {
            pstmt.setLong(x, link.id1);
            pstmt.setLong(x+1, link.id2);
            pstmt.setLong(x+2, link.link_type);
            pstmt.setByte(x+3, link.visibility);
            setBytesAsVarchar(pstmt, x+4, link.data);
            pstmt.setInt(x+5, link.version);
            pstmt.setLong(x+6, link.time);
            x += 7;
        }

        int nrows = pstmt.executeUpdate();
        if (nrows != links.size()) {
            String s = "addBulkLinks insert of " + links.size() + " links" +
                    " returned " + nrows;
            logger.error(s);
            throw new RuntimeException(s);
        }

        if (must_close_pstmt)
            pstmt.close();
    }

    private void setBytesAsVarchar(PreparedStatement pstmt, int i, byte[] bytes) throws SQLException {
        pstmt.setString(i, new String(bytes, StandardCharsets.US_ASCII));
    }

    private byte[] getVarcharAsBytes(ResultSet rs, int i) throws SQLException, IOException {
        return rs.getString(i).getBytes(StandardCharsets.US_ASCII);
    }
}

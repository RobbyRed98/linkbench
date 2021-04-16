/* * LinkStore for generic SQL
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

abstract class LinkStoreSql extends GraphStore {

  /* database server configuration keys */
  public static final String CONFIG_HOST = "host";
  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_USER = "user";
  public static final String CONFIG_PASSWORD = "password";

  String linktable;
  String counttable;
  String nodetable;

  // This only supports a single dbid to make reuse of prepared statements easier
  // TODO -- don't hardwire the value
  String init_dbid = "";

  String host;
  String user;
  String pwd;
  String port;

  // Use read-only and read-write connections and statements to avoid toggling
  // auto-commit.
  Connection conn_ac0, conn_ac1;
  Statement stmt_ac0, stmt_ac1;
  // for addNode, in autocommit mode
  PreparedStatement pstmt_add_node_1, pstmt_add_node_n;
  // for getNode, in autocommit mode
  PreparedStatement pstmt_get_node;
  // for updateNode, in autocommit mode
  PreparedStatement pstmt_update_node;
  // for deleteNode, in autocommit mode
  PreparedStatement pstmt_delete_node;

  // for addBulkLinks, in autocommit mode
  PreparedStatement pstmt_add_bulk_links_n;
  // for addBulkCounts, in autocommit mode
  PreparedStatement pstmt_add_bulk_counts_n;
  // for countLinks, in autocommit mode
  PreparedStatement pstmt_count_links;
  // for getLinkList, in autocommit mode
  PreparedStatement pstmt_get_link_list;
  // for getLink, in autocommit mode
  PreparedStatement pstmt_get_link;
  // for deleteLink, not in autocommit mode
  PreparedStatement pstmt_delete_link_upd_link;
  PreparedStatement pstmt_delete_link_del_link;
  // for addLink, not in autocommit mode
  PreparedStatement pstmt_add_link_ins_link;
  PreparedStatement pstmt_add_link_inc_count;
  // for updateLink, not in autocommit mode
  PreparedStatement pstmt_update_link_upd_link;
  // used in several places to increment count, not in autocommit mode
  PreparedStatement pstmt_link_inc_count;
  PreparedStatement pstmt_link_get_for_update;

  protected Phase phase;

  /**
   * Set of all JDBC SQLState strings that indicate a transient error
   * that should be handled by retrying
   */
  protected HashSet<String> retrySQLStates = populateRetrySQLStates();

  protected final Logger logger = Logger.getLogger();

  abstract HashSet<String> populateRetrySQLStates();
  abstract boolean isDupKeyError(SQLException ex);
  abstract String getJdbcUrl();
  abstract String getJdbcClassName();
  abstract String getJdbcOptions();
  // public void resetNodeStore(String dbid, long startID) throws SQLException;

  // This assumes the SQL engine has something like "insert on dup key update"
  // If not code can be changed to have an abstract method for doing this operation.
  abstract PreparedStatement makeAddLinkIncCountPS() throws SQLException;

  abstract void addLinkChangeCount(String dbid, Link l, int base_count, PreparedStatement pstmt)
      throws SQLException;

  // Don't implement his here as the DBMS might use a hint 
  abstract PreparedStatement makeGetLinkListPS() throws SQLException;

  abstract String getDefaultPort();
    
  public LinkStoreSql() {
    super();
  }

  public LinkStoreSql(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase, int threadId) {
    super.initialize(props, currentPhase, threadId);
    counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
    if (counttable.equals("")) {
      String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    nodetable = props.getProperty(Config.NODE_TABLE);
    if (nodetable.equals("")) {
      // For now, don't assume that nodetable is provided
      String msg = "Error! " + Config.NODE_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);

    host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
    user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
    pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);

    if (port == null || port.equals(""))
      port = getDefaultPort();

    phase = currentPhase;

    // connect
    try {
      openConnection();
    } catch (SQLException e) {
      logger.error(e);
      throw new RuntimeException("Connection error");
    }
  }

  protected PreparedStatement makeDeleteNodePS() throws SQLException {
    String sql = "DELETE FROM " + init_dbid + "." + nodetable + " " +
                 "WHERE id=? and type =?";
    logger.debug("deleteNode PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  protected PreparedStatement makeUpdateNodePS() throws SQLException {
    String sql = "UPDATE " + init_dbid + "." + nodetable +
                 " SET version=?, time=?, data=?" +
                 " WHERE id=? AND type=?";
    logger.debug("updateNode PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  protected PreparedStatement makeGetNodePS() throws SQLException {
    String sql = "SELECT id, type, version, time, data " +
      "FROM " + init_dbid + "." + nodetable + " " + "WHERE id=?";
    logger.debug("getNode PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  protected PreparedStatement makeAddNodePS(int num_entries) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO " + init_dbid + "." + nodetable + " " +
        "(type, version, time, data) " +
        "VALUES ");

    for (int x=0; x < num_entries; x++) {
      sql.append("(?, ?, ?, ?)");
      if (x < (num_entries-1))
        sql.append(",");
    }
    String sql_str = sql.toString();

    if (num_entries == 1)
      logger.debug("addNode PS: " + sql_str);

    return conn_ac1.prepareStatement(sql_str, PreparedStatement.RETURN_GENERATED_KEYS);
  }

  protected PreparedStatement makeUpdateLinkPS() throws SQLException {
    String sql = "UPDATE " + init_dbid + "." + linktable +
                 " SET visibility = ?," +
                 "     data = ?," +
                 "     version = ?," +
                 "     time = ?" +
                 " WHERE id1 = ? AND id2 = ? AND link_type = ?";
    logger.debug("updateLink PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeAddLinksPS(int num_entries, boolean autocommit) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO " + init_dbid + "." + linktable + " " +
        "(id1, id2, link_type, visibility, data, version, time) " +
        "VALUES ");

    for (int x=0; x < num_entries; x++) {
      sql.append("(?, ?, ?, ?, ?, ?, ?)");
      if (x < (num_entries-1))
        sql.append(",");
    }
    String sql_str = sql.toString();

    if (!autocommit)
      logger.debug("addLinks PS: " + sql_str);

    if (autocommit)
      return conn_ac1.prepareStatement(sql_str);
    else
      return conn_ac0.prepareStatement(sql_str);
  }

  protected PreparedStatement makeAddBulkCountsPS(int num_entries) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO " + init_dbid + "." + counttable + " " +
        "(id, link_type, time, version, count) " +
        "VALUES ");

    for (int x=0; x < num_entries; x++) {
      sql.append("(?, ?, ?, ?, ?)");
      if (x < (num_entries-1))
        sql.append(",");
    }
    String sql_str = sql.toString();

    if (num_entries == 1)
      logger.debug("addBulkCounts PS: " + sql_str);

    return conn_ac1.prepareStatement(sql_str);
  }

  protected PreparedStatement makeCountLinksPS() throws SQLException {
    String sql = "SELECT count from " + init_dbid + "." + counttable +
                 " WHERE id = ? AND link_type = ?";

    logger.debug("countLinks PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  protected PreparedStatement makeGetLinkPS() throws SQLException {
    String sql = "SELECT id1, id2, link_type, visibility, data, version, time" +
                 " FROM " + init_dbid + "." + linktable +
                 " WHERE id1 = ? AND link_type = ? AND id2 = ?";
    logger.debug("getLink PS: " + sql);
    return conn_ac1.prepareStatement(sql);
  }

  // This assumes the DBMS supports FOR UPDATE
  protected PreparedStatement makeLinkGetForUpdatePS() throws SQLException {
    String sql = "SELECT visibility" +
                 " FROM " + init_dbid + "." + linktable +
                 " WHERE id1 = ? AND id2 = ? AND link_type = ? FOR UPDATE";
    logger.debug("linkGetForUpdate PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeDeleteLinkUpdLinkPS() throws SQLException {
    String sql = "UPDATE " + init_dbid + "." + linktable +
                 " SET visibility = " + VISIBILITY_HIDDEN +
                 " WHERE id1 = ? AND id2 = ? AND link_type = ?";
    logger.debug("deleteLinkUpdLink PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeDeleteLinkDelLinkPS() throws SQLException {
    String sql = "DELETE FROM " + init_dbid + "." + linktable +
                 " WHERE id1 = ? AND id2 = ? AND link_type = ?";
    logger.debug("deleteLinkDelLink PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  protected PreparedStatement makeLinkIncCountPS() throws SQLException {
    String sql = "UPDATE " + init_dbid + "." + counttable +
                 " SET count = count + ?," +
                 "     version = version + 1," +
                 "     time = ?" +
                 " WHERE id = ? AND link_type = ?";
    logger.debug("linkIncCount PS: " + sql);
    return conn_ac0.prepareStatement(sql);
  }

  private void setNull() {
    stmt_ac0 = null;
    stmt_ac1 = null;
    pstmt_add_node_1 = null;
    pstmt_add_node_n = null;
    pstmt_get_node = null;
    pstmt_update_node = null;
    pstmt_delete_node = null;
    pstmt_add_bulk_links_n = null;
    pstmt_add_bulk_counts_n = null;
    pstmt_count_links = null;
    pstmt_get_link_list = null;
    pstmt_get_link = null;
    pstmt_delete_link_upd_link = null;
    pstmt_delete_link_del_link = null;
    pstmt_add_link_ins_link = null;
    pstmt_add_link_inc_count = null;
    pstmt_update_link_upd_link = null;
    pstmt_link_inc_count = null;
    pstmt_link_get_for_update = null;
    conn_ac0 = null;
    conn_ac1 = null;
  }

  // connects to test database
  private void openConnection() throws SQLException {
    setNull();

    String jdbcUrl = getJdbcUrl();

    try {
      Class.forName(getJdbcClassName()).getConstructor().newInstance();
    } catch (Exception ex) {
      String exStr = ex.toString();
      logger.error("Error while loading driver as (" + getJdbcClassName() + "): " + exStr);
      throw new RuntimeException("Cannot load driver(" + getJdbcClassName() + "): " + exStr);
    }

    // TODO are these valid for PG?
    jdbcUrl += getJdbcOptions();

    conn_ac0 = DriverManager.getConnection(jdbcUrl, user, pwd);
    conn_ac0.setAutoCommit(false);

    conn_ac1 = DriverManager.getConnection(jdbcUrl, user, pwd);
    conn_ac1.setAutoCommit(true);

    //System.err.println("connected");
    stmt_ac0 = conn_ac0.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);
    stmt_ac1 = conn_ac1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);

    // TODO if dbid is known then prepared statements can be created here

    // TODO check metadata pgsql JDBC driver support getGeneratedKeys() - probably not
    //dmd = conn.getMetadata() and then run dmd.supportsGetGeneratedKeys()
  }

  protected void checkDbid(String dbid) throws SQLException {
    // TODO make really prepared, use PG specific calls
    if (init_dbid.equals("")) {
      logger.debug("checkDbid creates PS for " + dbid);
      init_dbid = dbid;
      pstmt_add_node_1 = makeAddNodePS(1);
      pstmt_add_node_n = makeAddNodePS(bulkLoadBatchSize());
      pstmt_get_node = makeGetNodePS();
      pstmt_update_node = makeUpdateNodePS();
      pstmt_delete_node = makeDeleteNodePS();
      pstmt_add_bulk_links_n = makeAddLinksPS(bulkLoadBatchSize(), true);
      pstmt_add_bulk_counts_n = makeAddBulkCountsPS(bulkLoadBatchSize());
      pstmt_count_links = makeCountLinksPS();
      pstmt_get_link_list = makeGetLinkListPS();
      pstmt_get_link = makeGetLinkPS();
      pstmt_delete_link_upd_link = makeDeleteLinkUpdLinkPS();
      pstmt_delete_link_del_link = makeDeleteLinkDelLinkPS();
      pstmt_add_link_ins_link = makeAddLinksPS(1, false);
      pstmt_add_link_inc_count = makeAddLinkIncCountPS();
      pstmt_update_link_upd_link = makeUpdateLinkPS();
      pstmt_link_inc_count = makeLinkIncCountPS();
      pstmt_link_get_for_update = makeLinkGetForUpdatePS();
    } else if (!init_dbid.equals(dbid)) {
      logger.error("checkDbid cannot switch from " + init_dbid + " to " + dbid);
      throw new RuntimeException("cannot switch dbid from ::" +
                                 init_dbid + ":: to ::" + dbid + "::");
    }
  }

  @Override
  public void close() {
    try {
      init_dbid = "";
      if (stmt_ac0 != null) stmt_ac0.close();
      if (stmt_ac1 != null) stmt_ac1.close();
      if (pstmt_add_node_1 != null) pstmt_add_node_1.close();
      if (pstmt_add_node_n != null) pstmt_add_node_n.close();
      if (pstmt_get_node != null) pstmt_get_node.close();
      if (pstmt_update_node != null) pstmt_update_node.close();
      if (pstmt_delete_node != null) pstmt_delete_node.close();
      if (pstmt_add_bulk_links_n != null) pstmt_add_bulk_links_n.close();
      if (pstmt_add_bulk_counts_n != null) pstmt_add_bulk_links_n.close();
      if (pstmt_count_links != null) pstmt_count_links.close();
      if (pstmt_get_link_list != null) pstmt_get_link_list.close();
      if (pstmt_get_link != null) pstmt_get_link.close();
      if (pstmt_delete_link_upd_link != null) pstmt_delete_link_upd_link.close();
      if (pstmt_delete_link_del_link != null) pstmt_delete_link_del_link.close();
      if (pstmt_add_link_ins_link != null) pstmt_add_link_ins_link.close();
      if (pstmt_add_link_inc_count != null) pstmt_add_link_inc_count.close();
      if (pstmt_update_link_upd_link != null) pstmt_update_link_upd_link.close();
      if (pstmt_link_inc_count != null) pstmt_link_inc_count.close();
      if (pstmt_link_get_for_update != null) pstmt_link_get_for_update.close();
      if (conn_ac0 != null) conn_ac0.close();
      if (conn_ac1 != null) conn_ac1.close();
      setNull();
    } catch (SQLException e) {
      logger.error("Error while closing SQL connection: ", e);
    }
  }

  public void clearErrors(int threadID) {
    logger.info("Reopening SQL connection in threadID " + threadID);

    try {
      if (conn_ac0 != null) {
        conn_ac0.close();
      }
      if (conn_ac1 != null) {
        conn_ac1.close();
      }

      openConnection();
    } catch (Throwable e) {
      e.printStackTrace();
      return;
    }
  }

  private SQLException getSQLException(SQLException ex, String msg) {
    if (ex != null)
      return ex;
    else
      return new SQLException(msg);
  }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex SQLException thrown by PgSQL JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processSQLException(SQLException ex, String op) {
    boolean retry = retrySQLStates.contains(ex.getSQLState());
    String msg = "SQLException thrown by SQL driver during: " + op + ".  ";
    msg += "Message was: '" + ex.getMessage() + "'.  ";
    msg += "SQLState was: " + ex.getSQLState() + ".  ";

    GraphStore.incError(ex.getErrorCode());

    logger.error("SQLException from " + op + ": " + ex);

    if (retry) {
      msg += "Error is probably transient, retrying operation.";
      logger.warn(msg);
    } else {
      msg += "Error is probably non-transient, will abort operation.";
      logger.error(msg);
    }
    return retry;
  }

  // get count for testing purpose
  protected void testCount(String dbid, String assoctable, String counttable,
                           long id, long link_type) throws SQLException {

    String select1 = "SELECT COUNT(id2) as ct" +
                     " FROM " + dbid + "." + assoctable +
                     " WHERE id1 = " + id +
                     " AND link_type = " + link_type +
                     " AND visibility = " + VISIBILITY_DEFAULT;

    String select2 = "SELECT COALESCE (SUM(count), 0) as ct" +
                     " FROM " + dbid + "." + counttable +
                     " WHERE id = " + id +
                     " AND link_type = " + link_type;

    int r1=-1, r2=-1;

    conn_ac0.rollback();

    ResultSet result = stmt_ac0.executeQuery(select1);
    if (result.next())
      r1 = result.getInt("ct");

    result = stmt_ac0.executeQuery(select2);
    if (result.next())
      r2 = result.getInt("ct");

    conn_ac0.rollback();

    if (r1 != r2) {
      String s = "Data inconsistency between " + assoctable +
                 " and " + counttable + " for id=" + id +
                 " and link_type= " + link_type +
                 " with counts " + r1 + " and " + r2;
      logger.error(s);
      throw new RuntimeException(s);
    }
  }

  protected LinkWriteResult newLinkLoop(String dbid, Link l, boolean noinverse,
                                        boolean insert_first, String caller) throws SQLException {
    boolean do_insert = insert_first;
    SQLException last_ex = null;
    RetryCounter rc_add = new RetryCounter(retry_add_link, max_add_link);
    RetryCounter rc_upd = new RetryCounter(retry_update_link, max_update_link);

    // Sorry this is complicated. But it allows addLink to work when the link exists by switching
    // to update. And it allows updateLink to work when the link doesn't exist by switching to
    // insert.

    while (true) {

      try {
        if (do_insert) {
          if (!rc_add.inc(caller, logger))
            throw getSQLException(last_ex, caller);
          addLinkImpl(dbid, l, noinverse);
          return LinkWriteResult.LINK_INSERT;

        } else {
          if (!rc_upd.inc(caller, logger))
            throw getSQLException(last_ex, caller);
          LinkWriteResult wr = updateLinkImpl(dbid, l, noinverse);

          if (wr == LinkWriteResult.LINK_NO_CHANGE || wr == LinkWriteResult.LINK_UPDATE) {
            return wr;
          } else if (wr == LinkWriteResult.LINK_NOT_DONE) {
            // Row does not exist, switch to insert
            do_insert = true;
            retry_upd_to_add.incrementAndGet();
            logger.debug("newLinkLoop upd_to_add for id1=" + l.id1 + " id2=" + l.id2 +
                         " link_type=" + l.link_type + " gap " + (l.id2 - l.id1));
          } else {
            String s = "newLinkLoop bad result for update(" + wr + ") with id1=" + l.id1 +
                       " id2=" + l.id2 + " link_type=" + l.link_type;
            logger.error(s);
            throw new RuntimeException(s);
          }
        }

      } catch (SQLException ex) {
        last_ex = ex;
        conn_ac0.rollback();

        if (isDupKeyError(ex)) {
          // Row exists, switch to update
          do_insert = false;
          retry_add_to_upd.incrementAndGet();
          logger.debug("newLinkLoop add_to_upd for id1=" + l.id1 + " id2=" + l.id2 +
                       " link_type=" + l.link_type + " gap " + (l.id2 - l.id1));
        } else if (!processSQLException(ex, caller)) {
          throw ex;
        }
        // At this point the insert or update can be retried
      }
    }
  }

  @Override
  public LinkWriteResult addLink(String dbid, Link l, boolean noinverse) throws SQLException {
    return newLinkLoop(dbid, l, noinverse, true, "addLink");
  }

  protected void addLinkImpl(String dbid, Link l, boolean noinverse) throws SQLException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("addLink enter for " + l.id1 + "." + l.id2 + "." + l.link_type);

    pstmt_add_link_ins_link.setLong(1, l.id1);
    pstmt_add_link_ins_link.setLong(2, l.id2);
    pstmt_add_link_ins_link.setLong(3, l.link_type);
    pstmt_add_link_ins_link.setByte(4, l.visibility);
    pstmt_add_link_ins_link.setBytes(5, l.data);
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

  protected int getVisibilityForUpdate(long id1, long link_type, long id2, String caller)
      throws SQLException {

    pstmt_link_get_for_update.setLong(1, id1);
    pstmt_link_get_for_update.setLong(2, id2);
    pstmt_link_get_for_update.setLong(3, link_type);
    ResultSet result = pstmt_link_get_for_update.executeQuery();

    int visibility = VISIBILITY_NOT_FOUND;
    if (result.next()) {
      visibility = result.getInt(1);
      if (visibility != VISIBILITY_DEFAULT && visibility != VISIBILITY_HIDDEN) {
        String s = "Bad value for visibility=" + visibility + " with " +
                   "id1=" + id1 + " id2=" + id2 + " link_type=" + link_type;
        logger.error(s);
        throw new RuntimeException(s);
      }
    }
    result.close();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(String.format("getVisibility for " + caller +
                                 "(%d, %d, %d) visibility = %d",
                                 id1, link_type, id2, visibility));
    }

    return visibility;
  }

  @Override
  public LinkWriteResult updateLink(String dbid, Link l, boolean noinverse) throws SQLException {
    return newLinkLoop(dbid, l, noinverse, false, "updateLink");
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
    pstmt_update_link_upd_link.setBytes(2, l.data);
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

  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                            boolean noinverse, boolean expunge) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_delete_link, max_delete_link);
    SQLException last_ex = null;

    while (true) {
      // Enforce limit on retries
      if (!rc.inc("deleteLink", logger))
        throw getSQLException(last_ex, "deleteLink");

      try {
        return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
      } catch (SQLException ex) {
        last_ex = ex;
        conn_ac0.rollback();

        if (!processSQLException(ex, "deleteLink")) {
          throw ex;
        }
        // At this point the insert or update can be retried
      }
    }
  }

  protected boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
                                   boolean noinverse, boolean expunge) throws SQLException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("deleteLink " + id1 + "." + id2 + "." + link_type);

    // First do a select to check if the link is not there, is there and
    // hidden, or is there and visible;
    // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
    // In case of VISIBILITY_DEFAULT, later we need to mark the link as
    // hidden, and update counttable.
    // We lock the row exclusively because we rely on getting the correct
    // value of visible to maintain link counts.  Without the lock,
    // a concurrent transaction could also see the link as visible and
    // we would double-decrement the link count.
    //
    int visibility = getVisibilityForUpdate(id1, link_type, id2, "deleteLink");
    boolean found = (visibility != VISIBILITY_NOT_FOUND);

    if (!found || (visibility == VISIBILITY_HIDDEN && !expunge)) {
      logger.trace("deleteLinkImpl row not found");
      conn_ac0.rollback();
      return found;
    }

    // Only update count if link is present and visible
    boolean updateCount = (visibility != VISIBILITY_HIDDEN);

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

    // update count table
    // TODO this used to handle the case where the count table didn't have a row.
    // I am not sure why that is needed and will see if this works without that.
    pstmt_link_inc_count.setInt(1, -1); // count = count - 1
    pstmt_link_inc_count.setLong(2, (new Date()).getTime());
    pstmt_link_inc_count.setLong(3, id1);
    pstmt_link_inc_count.setLong(4, link_type);

    update_res = pstmt_link_inc_count.executeUpdate();
    if (update_res != 1) {
      String s = "deleteLink update count failed for id1=" + id1 +
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

  // lookup using id1, type, id2
  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2) throws SQLException, IOException {
    RetryCounter rc = new RetryCounter(retry_get_link, max_get_link);
    SQLException last_ex = null;

    while (true) {
      // Enforce limit on retries
      if (!rc.inc("getLink", logger))
        throw getSQLException(last_ex, "getLink");

      try {
        return getLinkImpl(dbid, id1, link_type, id2);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "getLink"))
          throw ex;
      }
    }
  }

  protected Link getLinkImpl(String dbid, long id1, long link_type, long id2) throws SQLException, IOException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("getLink for id1=" + id1 + ", link_type=" + link_type +
                   ", id2=" + id2);
    }

    pstmt_get_link.setLong(1, id1);
    pstmt_get_link.setLong(2, link_type);
    pstmt_get_link.setLong(3, id2);
    ResultSet rs = pstmt_get_link.executeQuery();

    if (rs.next()) {
      Link l = createLinkFromRow(rs);
      rs.close();
      return l;
    } else {
      rs.close();
      logger.trace("getLink found now row");
      return null;
    }
  }

  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type, long[] id2s) throws SQLException, IOException {
    RetryCounter rc = new RetryCounter(retry_multigetlinks, max_multigetlinks);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("multigetLinks", logger))
        throw getSQLException(last_ex, "multigetLinks");

      try {
        return multigetLinksImpl(dbid, id1, link_type, id2s);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "multigetLinks"))
          throw ex;
      }
    }
  }

  protected Link[] multigetLinksImpl(String dbid, long id1, long link_type, long[] id2s) throws SQLException, IOException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("multigetLinks for id1=" + id1 + ", link_type=" + link_type);

    StringBuilder query = new StringBuilder();
    query.append("SELECT id1, id2, link_type, visibility, data, version, time" +
        " FROM " + dbid + "." + linktable +
        " WHERE id1 = " + id1 + " AND link_type = " + link_type +
        " and id2 IN (");

    boolean first = true;
    for (long id2: id2s) {
      if (first)
        first = false;
      else
        query.append(",");

      query.append(id2);
    }
    query.append(")");
    String sql = query.toString();

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("multiget query is " + sql);

    ResultSet rs = stmt_ac1.executeQuery(sql);
    List<Link> links = new ArrayList<>();

    while (rs.next()) {
      Link l = createLinkFromRow(rs);
      links.add(l);
    }

    if (links.size() > 0) {
      if (Level.TRACE.isGreaterOrEqual(debuglevel))
        logger.trace("multigetLinks found " + links.size() + " rows ");
      return links.toArray(new Link[links.size()]);
    } else {
      logger.trace("multigetLinks row not found");
      return new Link[0];
    }
  }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type) throws SQLException, IOException {
    // Retry logic in getLinkList
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit) throws SQLException, IOException {
    RetryCounter rc = new RetryCounter(retry_get_link_list, max_get_link_list);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("getLinkList", logger))
        throw getSQLException(last_ex, "getLinkList");

      try {
        return getLinkListImpl(dbid, id1, link_type, minTimestamp,
                               maxTimestamp, offset, limit);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "getLinkListImpl"))
          throw ex;
      }
    }
  }

  protected Link[] getLinkListImpl(String dbid, long id1, long link_type,
                                   long minTimestamp, long maxTimestamp,
                                   int offset, int limit) throws SQLException, IOException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("getLinkList for id1=" + id1 + ", link_type=" + link_type +
                   " minTS=" + minTimestamp + ", maxTS=" + maxTimestamp +
                   " offset=" + offset + ", limit=" + limit);
    }

    pstmt_get_link_list.setLong(1, id1);
    pstmt_get_link_list.setLong(2, link_type);
    pstmt_get_link_list.setLong(3, minTimestamp);
    pstmt_get_link_list.setLong(4, maxTimestamp);
    pstmt_get_link_list.setInt(5, limit);
    pstmt_get_link_list.setInt(6, offset);

    ResultSet rs = pstmt_get_link_list.executeQuery();
    List<Link> links = new ArrayList<>();

    while (rs.next()) {
      // logger.trace("getLinkList next result row");
      Link l = createLinkFromRow(rs);
      links.add(l);
    }

    if (links.size() > 0) {
      if (Level.TRACE.isGreaterOrEqual(debuglevel))
        logger.trace("getLinkList found " + links.size() + " rows ");
      return links.toArray(new Link[links.size()]);
    } else {
      logger.trace("getLinkList found no row");
      return null;
    }
  }

  protected Link createLinkFromRow(ResultSet rs) throws SQLException, IOException {
    Link l = new Link();
    l.id1 = rs.getLong(1);
    l.id2 = rs.getLong(2);
    l.link_type = rs.getLong(3);
    l.visibility = rs.getByte(4);
    l.data = rs.getBytes(5);
    l.version = rs.getInt(6);
    l.time = rs.getLong(7);
    return l;
  }

  // count the #links
  @Override
  public long countLinks(String dbid, long id1, long link_type) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_count_links, max_count_links);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("countLinks", logger))
        throw getSQLException(last_ex, "countLinks");

      try {
        return countLinksImpl(dbid, id1, link_type);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "countLinks"))
          throw ex;
      }
    }
  }

  protected long countLinksImpl(String dbid, long id1, long link_type) throws SQLException {
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("countLinks for id1=" + id1 + " and link_type=" + link_type);

    pstmt_count_links.setLong(1, id1);
    pstmt_count_links.setLong(2, link_type);

    ResultSet rs = pstmt_count_links.executeQuery();

    if (rs.next()) {
      long count = rs.getLong(1);
      rs.close();
      return count;
    } else {
      logger.trace("countLinks found no row");
      rs.close();
      return 0;
    }
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_add_bulk_links, max_add_bulk_links);
    SQLException last_ex = null;

    // TODO - count and limit retries
    while (true) {
      if (!rc.inc("addBulkLinks", logger))
        throw getSQLException(last_ex, "addBulkLinks");

      try {
        addBulkLinksImpl(dbid, links, noinverse);
        return;
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "addBulkLinks"))
          throw ex;
      }
    }
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
      pstmt.setBytes(x+4, link.data);
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

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_add_bulk_counts, max_add_bulk_counts);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("addBulkCounts", logger))
        throw getSQLException(last_ex, "addBulkCounts");

      try {
        addBulkCountsImpl(dbid, counts);
        return;
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "addBulkCounts"))
          throw ex;
      }
    }
  }

  protected void addBulkCountsImpl(String dbid, List<LinkCount> counts) throws SQLException {
    // TODO - MySQL code used REPLACE here. Is that needed?
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("addBulkCounts: " + counts.size() + " counts");

    PreparedStatement pstmt;
    boolean must_close_pstmt = false;

    if (counts.size() == bulkLoadBatchSize())
      pstmt = pstmt_add_bulk_counts_n;
    else {
      pstmt = makeAddBulkCountsPS(counts.size());
      must_close_pstmt = true;
    }

    int x = 1;
    for (LinkCount count: counts) {
      pstmt.setLong(x, count.id1);
      pstmt.setLong(x+1, count.link_type);
      pstmt.setLong(x+2, count.time);
      pstmt.setLong(x+3, count.version);
      pstmt.setLong(x+4, count.count);
      x += 5;
    }

    int nrows = pstmt.executeUpdate();
    if (nrows != counts.size()) {
      String s = "addBulkCounts insert of " + counts.size() + " counts " +
                 " returned " + nrows;
      logger.error(s);
      throw new RuntimeException(s);
    }

    if (must_close_pstmt)
      pstmt.close();
  }

  protected void checkNodeTableConfigured() {
    if (this.nodetable == null) {
      logger.error("nodetable not set");
      throw new RuntimeException("nodetable not set");
    }
  }

  @Override
  public long addNode(String dbid, Node node) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_add_node, max_add_node);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("addNode", logger))
        throw getSQLException(last_ex, "addNode");

      try {
        long ids[] = bulkAddNodesImpl(dbid, Collections.singletonList(node));
        if (ids.length != 1) {
          String s = "addNode for " + node.id + " expected 1 returned " + ids.length;
          logger.error(s);
          throw new RuntimeException(s);
        }
        return ids[0];
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "addNode"))
          throw ex;
      }
    }
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_bulk_add_nodes, max_bulk_add_nodes);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("bulkAddNodes", logger))
        throw getSQLException(last_ex, "bulkAddNodes");

      try {
        return bulkAddNodesImpl(dbid, nodes);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "bulkAddNodes"))
          throw ex;
      }
    }
  }

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
      pstmt.setBytes(x+3, node.data);
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

  @Override
  public Node getNode(String dbid, int type, long id) throws SQLException, IOException {
    RetryCounter rc = new RetryCounter(retry_get_node, max_get_node);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("getNode", logger))
        throw getSQLException(last_ex, "getNode");

      try {
        return getNodeImpl(dbid, type, id);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "getNode"))
          throw ex;
      }
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
      Node res = new Node(rs.getLong(1), rs.getInt(2), rs.getLong(3), rs.getInt(4), rs.getBytes(5));

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

  @Override
  public boolean updateNode(String dbid, Node node) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_update_node, max_update_node);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("updateNode", logger))
        throw getSQLException(last_ex, "updateNode");

      try {
        return updateNodeImpl(dbid, node);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "updateNode"))
          throw ex;
      }
    }
  }

  protected boolean updateNodeImpl(String dbid, Node node) throws SQLException {
    checkNodeTableConfigured();
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("updateNode for id " + node.id);

    pstmt_update_node.setLong(1, node.version);
    pstmt_update_node.setInt(2, node.time);
    pstmt_update_node.setBytes(3, node.data);
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

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws SQLException {
    RetryCounter rc = new RetryCounter(retry_delete_node, max_delete_node);
    SQLException last_ex = null;

    while (true) {
      if (!rc.inc("deleteNode", logger))
        throw getSQLException(last_ex, "deleteNode");

      try {
        return deleteNodeImpl(dbid, type, id);
      } catch (SQLException ex) {
        last_ex = ex;

        if (!processSQLException(ex, "deleteNode"))
          throw ex;
      }
    }
  }

  protected boolean deleteNodeImpl(String dbid, int type, long id) throws SQLException {
    checkNodeTableConfigured();
    checkDbid(dbid);

    if (Level.TRACE.isGreaterOrEqual(debuglevel))
      logger.trace("deleteNode for id " + id);

    pstmt_delete_node.setLong(1, id);
    pstmt_delete_node.setInt(2, type);

    int rows = pstmt_delete_node.executeUpdate();
    if (rows == 0) {
      return false;
    } else if (rows == 1) {
      return true;
    } else {
      String s = "deleteNode should delete 1 or 0 but result was " + rows +
                 " for id=" + id + " and type=" + type;
      logger.error(s);
      throw new RuntimeException(s);
    }
  }

}

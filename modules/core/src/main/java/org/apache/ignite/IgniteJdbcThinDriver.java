/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import cn.myservice.MyConnectionService;
import cn.mysuper.model.MyUrlToken;
import cn.mysuper.service.IMyConnection;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.rpc.client.MySuperSqlFuncClient;

/**
 * JDBC driver thin implementation for In-Memory Data Grid.
 * <p>
 * Driver allows to get distributed data from Ignite cache using standard
 * SQL queries and standard JDBC API. It will automatically get only fields that
 * you actually need from objects stored in cache.
 * <h1 class="header">Limitations</h1>
 * Data in Ignite cache is usually distributed across several nodes,
 * so some queries may not work as expected since the query will be sent to each
 * individual node and results will then be collected and returned as JDBC result set.
 * Keep in mind following limitations (not applied if data is queried from one node only,
 * or data is fully co-located or fully replicated on multiple nodes):
 * <ul>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to
 *         {@link AffinityKey}
 *         javadoc for more details.
 *     </li>
 *     <li>
 *         Note that if you are connected to local or replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you are connected to partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <h1 class="header">SQL Notice</h1>
 * Driver allows to query data from several caches. Cache that driver is connected to is
 * treated as default schema in this case. Other caches can be referenced by their names.\
 *
 * <h1 class="header">Dependencies</h1>
 * JDBC driver is located in main Ignite JAR in {@code IGNITE_HOME/libs} folder.
 * <h1 class="header">Configuration</h1>
 *
 * <p>
 * JDBC connection URL has the following pattern:
 * {@code jdbc:ignite://<hostname>:<port>/}<br>
 * Note the following:
 * <ul>
 *     <li>Hostname is required.</li>
 *     <li>If port is not defined, {@code 10800} is used (default for Ignite thin client).</li>
 * </ul>
 * Other properties can be defined in {@link Properties} object passed to
 * {@link DriverManager#getConnection(String, Properties)} method:
 * <table class="doctable">
 *     <tr>
 *         <th>Name</th>
 *         <th>Description</th>
 *         <th>Default</th>
 *         <th>Optional</th>
 *     </tr>
 *     <tr>
 *         <td><b>ignite.jdbc.distributedJoins</b></td>
 *         <td>Flag to enable distributed joins.</td>
 *         <td>{@code false} (distributed joins are disabled)</td>
 *         <td>Yes</td>
 *     </tr>
 *     <tr>
 *         <td><b>ignite.jdbc.enforceJoinOrder</b></td>
 *         <td>Flag to enforce join order of tables in the query.</td>
 *         <td>{@code false} (enforcing join order is disabled)</td>
 *         <td>Yes</td>
 *     </tr>
 * </table>
 * <h1 class="header">Example</h1>
 * <pre name="code" class="java">
 * // Open JDBC connection.
 * Connection conn = DriverManager.getConnection("jdbc:ignite:thin//localhost:10800");
 *
 * // Query persons' names
 * ResultSet rs = conn.createStatement().executeQuery("select name from Person");
 *
 * while (rs.next()) {
 *     String name = rs.getString(1);
 *
 *     ...
 * }
 *
 * // Query persons with specific age
 * PreparedStatement stmt = conn.prepareStatement("select name, age from Person where age = ?");
 *
 * stmt.setInt(1, 30);
 *
 * ResultSet rs = stmt.executeQuery();
 *
 * while (rs.next()) {
 *     String name = rs.getString("name");
 *     int age = rs.getInt("age");
 *
 *     ...
 * }
 * </pre>
 */
public class IgniteJdbcThinDriver implements Driver {
    /** Driver instance. */
    private static final Driver INSTANCE = new IgniteJdbcThinDriver();

    /** Registered flag. */
    private static volatile boolean registered;

    static {
        register();
    }

    /** Major version. */
    private static final int MAJOR_VER = IgniteVersionUtils.VER.major();

    /** Minor version. */
    private static final int MINOR_VER = IgniteVersionUtils.VER.minor();

    /** {@inheritDoc} */
//    @Override public Connection connect(String url, Properties props) throws SQLException {
//        if (!acceptsURL(url))
//            return null;
//
//        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
//
//        connProps.init(url, props);
//
//        return new JdbcThinConnection(connProps);
//    }

    @Override public Connection connect(String url, Properties props) throws SQLException {
        System.out.println("*******************************");
        System.out.println(url);
        System.out.println("*******************************");
        MyUrlToken myUrlToken = null;
        Boolean isThinJdbc = false;
        long my_group_id = -1L;
        MyConnectionService myConnectionService = MyConnectionService.getInstance();
        if (myConnectionService != null) {
            IMyConnection myConnection = myConnectionService.getMyConnection();
            if (myConnection != null) {
                isThinJdbc = myConnection.isJdbcThin(url);
                myUrlToken = myConnection.getToken(url);

                if(myUrlToken != null)
                {
                    long group_id = MySuperSqlFuncClient.getGroupId(myUrlToken.getUserToken());
                    if (group_id == -1L)
                    {
                        try {
                            throw new Exception("userToken 没有连接权限！");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                    else
                    {
                        my_group_id = group_id;
                    }
                }
            }
        }

        if (isThinJdbc && my_group_id == -1L)
        {
            try {
                throw new Exception("userToken 没有连接权限！");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        if (myUrlToken == null) {
            myUrlToken = new MyUrlToken("", url);
        }

        if (!acceptsURL(myUrlToken.getUrl()))
            return null;

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(myUrlToken.getUrl(), props);

        JdbcThinConnection jdbcThinConnection = new JdbcThinConnection(connProps);
        jdbcThinConnection.setThinJdbc(isThinJdbc);
        if (myUrlToken.getUserToken() != null && !myUrlToken.getUserToken().equals("")) {
            jdbcThinConnection.setUserToken(myUrlToken.getUserToken());
            jdbcThinConnection.setGroup_id(my_group_id);
        }

        return jdbcThinConnection;
    }

    /** {@inheritDoc} */
    @Override public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(JdbcThinUtils.URL_PREFIX);
    }

    /** {@inheritDoc} */
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(url, info);

        return connProps.getDriverPropertyInfo();
    }

    /** {@inheritDoc} */
    @Override public int getMajorVersion() {
        return MAJOR_VER;
    }

    /** {@inheritDoc} */
    @Override public int getMinorVersion() {
        return MINOR_VER;
    }

    /** {@inheritDoc} */
    @Override public boolean jdbcCompliant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used.");
    }

    /**
     * @return Driver instance.
     */
    public static synchronized Driver register() {
        try {
            if (!registered) {
                DriverManager.registerDriver(INSTANCE);

                registered = true;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to register Ignite JDBC thin driver.", e);
        }

        return INSTANCE;
    }
}

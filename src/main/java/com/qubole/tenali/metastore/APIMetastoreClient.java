/*
 * Copyright (c) 2015. Qubole Inc
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
 *    limitations under the License.
 */

package com.qubole.tenali.metastore;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sakshibansal on 21/02/17.
 */
public class APIMetastoreClient implements IMetaStoreClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(APIMetastoreClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private URLFactory urlFactory;
    private final String apiToken;

    public APIMetastoreClient(int account,
                              String envAddr, String apiToken) throws Exception {
        if (envAddr.endsWith("/")) {
            envAddr = envAddr.substring(0, envAddr.length() - 1);
        }
        urlFactory = new URLFactory(envAddr, account);
        this.apiToken = apiToken;
    }

    @Override
    public boolean isCompatibleWith(HiveConf hiveConf) {
        throw new NotImplementedException();
    }

    @Override
    public void setHiveAddedJars(String s) {
        throw new NotImplementedException();
    }

    @Override
    public void reconnect() throws MetaException {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }

    @Override
    public void setMetaConf(String s, String s1) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public String getMetaConf(String s) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        throw new NotImplementedException();
    }

    @Override
    public Database getDatabase(String name)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException,
            TException, NoSuchObjectException {

        try {

            List<FieldSchema> columns = Lists.newArrayList();
            List<FieldSchema> partitions = Lists.newArrayList();
            Map<String, String> parameterMap = Maps.newHashMap();
            String serdeInput = "";
            String serdeOutput = "";

            URL url = urlFactory.getFieldsURL(dbName, tableName);
            String data = readData(url, apiToken);

            Map<String, List<Map<String, String>>> metadataInfo = MAPPER.readValue(data,
                    new TypeReference<Map<String, List<Map<String, String>>>>() { });

            List<Map<String, String>> columnInfoMaps = metadataInfo.get("columns");
            List<Map<String, String>> tableParamsMaps = metadataInfo.get("table_params");
            List<Map<String, String>> serdeInfoMaps = metadataInfo.get("serde_info");
            List<Map<String, String>> typeInfoMaps = metadataInfo.get("type_info");

            String table_type = "";
            String view_expanded_text = "";

            if (typeInfoMaps != null && typeInfoMaps.get(0) != null) {
                table_type = typeInfoMaps.get(0).get("table_type");
                view_expanded_text = typeInfoMaps.get(0).get("view_expanded_text");
            }

            for (Map<String, String> columnInfoMap : columnInfoMaps) {
                String name = columnInfoMap.get("name");
                String type = columnInfoMap.get("type");
                String comment = columnInfoMap.get("comment");
                FieldSchema fieldSchema = new FieldSchema(name, type, comment);
                int is_partition = Integer.parseInt(columnInfoMap.get("is_partition"));
                if (is_partition == 1) {
                    partitions.add(fieldSchema);
                } else {
                    columns.add(fieldSchema);
                }
            }

            for (Map<String, String> tableParamsMap : tableParamsMaps) {
                parameterMap.put(tableParamsMap.get("param_key"), tableParamsMap.get("param_value"));
            }

            for (Map<String, String> serdeInfoMap : serdeInfoMaps) {
                serdeInput = serdeInfoMap.get("serde_input");
                serdeOutput = serdeInfoMap.get("serde_output");
            }

            StorageDescriptor storageDescriptor = new StorageDescriptor(columns, "", serdeInput,
                    serdeOutput, false, -1, null, null, null, null);

            Table table = new Table(tableName, dbName, "", -1, -1, -1, storageDescriptor, partitions,
                    parameterMap, "", view_expanded_text, table_type);

            return table;

        } catch (MalformedURLException e) {
            LOGGER.error("Error while fetching column metadata: " + e.getMessage(), e);
            throw new MetaException("Error while fetching column metadata: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Error deserializing column data: " + e.getMessage(), e);
            throw new MetaException("Error deserializing column data: " + e.getMessage());
        }

    }

    /**
     * @deprecated
     */
    @Deprecated
    public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition appendPartition(String tableName, String dbName, List<String> partVals)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition appendPartition(String tableName, String dbName, String name)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition add_partition(Partition partition) throws InvalidObjectException,
            AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public int add_partitions(List<Partition> partitions) throws InvalidObjectException,
            AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> add_partitions(
            List<Partition> partitions, boolean ifNotExists, boolean needResults)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition getPartition(
            String tblName, String dbName, List<String> partVals)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition exchange_partition(
            Map<String, String> partitionSpecs, String sourceDb,
            String sourceTable, String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition getPartition(String dbName, String tblName, String name)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Partition getPartitionWithAuthInfo(
            String dbName, String tableName, List<String> pvals,
            String userName, List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> listPartitions(
            String dbName, String tblName, short maxParts)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> listPartitions(
            String dbName, String tblName, List<String> partVals, short maxParts)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short maxParts)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> listPartitionNames(
            String dbName, String tblName, List<String> partVals, short maxParts)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> listPartitionsByFilter(
            String dbName, String tblName, String filter, short maxParts)
            throws MetaException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
            throws MetaException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean listPartitionsByExpr(
            String dbName, String tblName, byte[] expr, String defaultPartitionname,
            short maxParts, List<Partition> result) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String dbName, String tableName, short s,
            String userName, List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> getPartitionsByNames(
            String dbName, String tblName, List<String> partNames)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> listPartitionsWithAuthInfo(
            String dbName, String tableName, List<String> partialPvals,
            short s, String userName, List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public void markPartitionForEvent(String dbName, String tblName,
                                      Map<String, String> partKVs, PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException,
            UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        throw new NotImplementedException();
    }

    @Override
    public boolean isPartitionMarkedForEvent(
            String dbName, String tblName, Map<String, String> partKVs,
            PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException,
            UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        throw new NotImplementedException();
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals)
            throws TException, MetaException {
        throw new NotImplementedException();
    }

    @Override
    public void createTable(Table tbl) throws AlreadyExistsException,
            InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alter_table(String defaultDatabaseName, String tblName, Table table)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alter_table(String s, String s1, Table table, boolean b)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void createDatabase(Database db) throws InvalidObjectException,
            AlreadyExistsException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void dropDatabase(String name) throws NoSuchObjectException,
            InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb,
                             boolean cascade) throws NoSuchObjectException,
            InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alterDatabase(String name, Database db)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropPartition(
            String dbName, String tblName, List<String> partVals, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropPartition(String s, String s1, List<String> list,
                                 PartitionDropOptions partitionDropOptions) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> dropPartitions(
            String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData, boolean ignoreProtection, boolean ifExists)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> dropPartitions(
            String s, String s1, List<ObjectPair<Integer, byte[]>> list,
            boolean b, boolean b1, boolean b2, boolean b3)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Partition> dropPartitions(
            String s, String s1, List<ObjectPair<Integer, byte[]>> list,
            PartitionDropOptions partitionDropOptions) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alter_partition(String dbName, String tblName, Partition newPart)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void renamePartition(String dbname, String name,
                                List<String> partVals, Partition newPart)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
            throws MetaException, TException, InvalidOperationException, UnknownDBException {
        throw new NotImplementedException();
    }

    @Override
    public void dropTable(String dbname, String tableName,
                          boolean deleteData, boolean ignoreUknownTab)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public void dropTable(String s, String s1, boolean b, boolean b1, boolean b2)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public void dropTable(String tableName, boolean deleteData)
            throws MetaException, UnknownTableException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public void dropTable(String dbname, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        throw new NotImplementedException();
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
            throws MetaException, TException, UnknownDBException {
        throw new NotImplementedException();
    }

    @Override
    public boolean tableExists(String tableName) throws MetaException,
            TException, UnknownDBException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern) throws MetaException {
        List<String> tables;
        try {
            URL url = urlFactory.gettbPatternURL(dbname, tablePattern);
            String data = readData(url, apiToken);
            tables = MAPPER.readValue(data, List.class);
        } catch (MalformedURLException e) {
            LOGGER.error("Error fetching tables metadata: " + e.getMessage(), e);
            throw new MetaException("Error fetching tables metadata: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Error deserializing table list: " + e.getMessage(), e);
            throw new MetaException("Error deserializing table list: " + e.getMessage());
        }
        return tables;
    }

    @Override
    public List<String> getAllTables(String dbname) throws MetaException {
        List<String> tables;
        try {
            URL url = urlFactory.gettableURL(dbname);
            String data = readData(url, apiToken);
            tables = MAPPER.readValue(data, List.class);
        } catch (MalformedURLException e) {
            LOGGER.error("Error fetching tables metadata: " + e.getMessage(), e);
            throw new MetaException("Error fetching tables metadata: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Error deserializing table list: " + e.getMessage(), e);
            throw new MetaException("Error deserializing table list: " + e.getMessage());
        }
        return tables;
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        List<FieldSchema> columns;
        try {
            URL url = urlFactory.getFieldsURL(db, tableName);
            String data = readData(url, apiToken);
            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            Map<String, List<FieldSchema>> apiResponse = mapper.readValue(data,
                    new TypeReference<Map<String, List<FieldSchema>>>() { });
            columns = apiResponse.get("columns");

        } catch (MalformedURLException e) {
            LOGGER.error("Error while fetching column metadata: " + e.getMessage(), e);
            throw new MetaException("Error while fetching column metadata: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Error deserializing column data: " + e.getMessage(), e);
            throw new MetaException("Error deserializing column data: " + e.getMessage());
        }
        return columns;
    }

    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, InvalidInputException {
        throw new NotImplementedException();
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean grant_role(
            String roleName, String userName, PrincipalType principalType,
            String grantor, PrincipalType grantorType, boolean grantOption)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Role> list_roles(
            String principalName, PrincipalType principalType) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(
            HiveObjectRef hiveObject, String userName, List<String> groupNames)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principalName, PrincipalType principalType, HiveObjectRef hiveObject)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public String getTokenStrForm() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void createFunction(Function func)
            throws InvalidObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void dropFunction(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException,
            InvalidInputException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        throw new NotImplementedException();
    }

    @Override
    public ValidTxnList getValidTxns(long l) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public long openTxn(String user) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {
        throw new NotImplementedException();
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        throw new NotImplementedException();
    }

    @Override
    public LockResponse lock(LockRequest request) throws NoSuchTxnException,
            TxnAbortedException, TException {
        throw new NotImplementedException();
    }

    @Override
    public LockResponse checkLock(long lockid) throws NoSuchTxnException,
            TxnAbortedException, NoSuchLockException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
        throw new NotImplementedException();
    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        throw new NotImplementedException();
    }

    @Override
    public void heartbeat(long txnid, long lockid) throws NoSuchLockException,
            NoSuchTxnException, TxnAbortedException, TException {
        throw new NotImplementedException();
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public void compact(String dbname, String tableName, String partitionName, CompactionType type)
            throws TException {
        throw new NotImplementedException();
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        throw new NotImplementedException();
    }

    @Override
    public void addDynamicPartitions(long l, String s, String s1, List<String> list)
            throws TException {
        throw new NotImplementedException();
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long l, int i, NotificationFilter notificationFilter)
            throws TException {
        throw new NotImplementedException();
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        throw new NotImplementedException();
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(
            GetPrincipalsInRoleRequest getPrincRoleReq) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public AggrStats getAggrColStatsFor(String s, String s1, List<String> list, List<String> list1)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest setPartitionsStatsRequest)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, InvalidInputException {
        throw new NotImplementedException();
    }

    public List<FieldSchema> getSchema(String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        throw new NotImplementedException();
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException, ConfigValSecurityException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public void createIndex(Index index, Table indexTable)
            throws InvalidObjectException, MetaException,
            NoSuchObjectException, TException, AlreadyExistsException {
        throw new NotImplementedException();
    }

    @Override
    public void alter_index(String dbName, String tblName, String indexName, Index index)
            throws InvalidOperationException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Index getIndex(String dbName, String tblName, String indexName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<Index> listIndexes(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public List<String> listIndexNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean dropIndex(String dbName, String tblName, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException,
            InvalidObjectException, MetaException, TException, InvalidInputException {
        throw new NotImplementedException();
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
            throws NoSuchObjectException,
            InvalidObjectException, MetaException, TException, InvalidInputException {
        throw new NotImplementedException();
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String dbName, String tableName, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tableName, List<String> partNames, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        throw new NotImplementedException();
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName)
            throws NoSuchObjectException, MetaException,
            InvalidObjectException, TException, InvalidInputException {
        throw new NotImplementedException();
    }

    private Partition deepCopy(Partition partition) {
        Partition copy = null;
        if (partition != null) {
            copy = new Partition(partition);
        }

        return copy;
    }

    private Database deepCopy(Database database) {
        Database copy = null;
        if (database != null) {
            copy = new Database(database);
        }

        return copy;
    }

    protected Table deepCopy(Table table) {
        Table copy = null;
        if (table != null) {
            copy = new Table(table);
        }

        return copy;
    }

    private Index deepCopy(Index index) {
        Index copy = null;
        if (index != null) {
            copy = new Index(index);
        }

        return copy;
    }

    private Type deepCopy(Type type) {
        Type copy = null;
        if (type != null) {
            copy = new Type(type);
        }

        return copy;
    }

    private FieldSchema deepCopy(FieldSchema schema) {
        FieldSchema copy = null;
        if (schema != null) {
            copy = new FieldSchema(schema);
        }

        return copy;
    }

    private Function deepCopy(Function func) {
        Function copy = null;
        if (func != null) {
            copy = new Function(func);
        }

        return copy;
    }

    protected PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
        PrincipalPrivilegeSet copy = null;
        if (pps != null) {
            copy = new PrincipalPrivilegeSet(pps);
        }

        return copy;
    }

    private List<Partition> deepCopyPartitions(List<Partition> partitions) {
        return this.deepCopyPartitions(partitions, (List) null);
    }

    private List<Partition> deepCopyPartitions(Collection<Partition> src, List<Partition> dest) {
        if (src == null) {
            return (List) dest;
        } else {
            if (dest == null) {
                dest = new ArrayList(src.size());
            }

            Iterator it = src.iterator();

            while (it.hasNext()) {
                Partition part = (Partition) it.next();
                ((List) dest).add(this.deepCopy(part));
            }

            return (List) dest;
        }
    }

    private List<Table> deepCopyTables(List<Table> tables) {
        ArrayList copy = null;
        if (tables != null) {
            copy = new ArrayList();
            Iterator it = tables.iterator();

            while (it.hasNext()) {
                Table tab = (Table) it.next();
                copy.add(this.deepCopy(tab));
            }
        }

        return copy;
    }

    protected List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
        ArrayList copy = null;
        if (schemas != null) {
            copy = new ArrayList();
            Iterator it = schemas.iterator();

            while (it.hasNext()) {
                FieldSchema schema = (FieldSchema) it.next();
                copy.add(this.deepCopy(schema));
            }
        }

        return copy;
    }

    /**
     * Created by sakshibansal on 03/03/17.
     */
    private class URLFactory {

        private String restRoot;
        private int account;

        private URLFactory(String envAddr, int account) throws IOException {
            restRoot = "https://" + envAddr + "/api/internal/hive";
            this.account = account;
            verifyURL(restRoot);
        }

        private void verifyURL(String url) throws IOException {
            final URLConnection connection = new URL(url).openConnection();
            // Check service availability
            connection.connect();
            return;
        }

        private URL gettableURL(String dbName) throws MalformedURLException {
            return new URL(restRoot + "/" + dbName.toLowerCase() + "?account_id=" + account);
        }

        private URL gettbPatternURL(String dbName, String tablePattern) throws MalformedURLException {
            return new URL(restRoot + "/" + dbName.toLowerCase()
                    + "?account_id=" + account + "&filter=" + tablePattern.toLowerCase());
        }

        private URL getFieldsURL(String dbName, String tblName) throws MalformedURLException {
            return new URL(restRoot + "/" + dbName.toLowerCase()
                    + "/" + tblName.toLowerCase() + "?account_id=" + account);
        }
    }

    private static String readData(URL url, String apiToken) throws MetaException {

        BufferedReader reader = null;
        HttpURLConnection conn = null;
        StringBuilder sb = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("X-AUTH-TOKEN", apiToken);
            conn.setRequestMethod("GET");
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            try {
                int respCode = conn.getResponseCode();
                BufferedReader breader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                // read the response body
                StringBuilder esb = new StringBuilder();
                String line;
                while ((line = breader.readLine()) != null) {
                    esb.append(line + "\n");
                }
                breader.close();
                LOGGER.error("Error fetching metadata from API: " + e);
                throw new MetaException("Error fetching metadata from API: " + e.getMessage());
            } catch (IOException ex) {
                throw new MetaException("Error fetching error stream from failed metadata API call: "
                        + e.getMessage());
            }
            // close the errorstream
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                    return sb.toString();
                } catch (IOException e) {
                    throw new MetaException("Error while fetching metadata from API: " + e.getMessage());
                }
            }
        }
        return null;
    }
}
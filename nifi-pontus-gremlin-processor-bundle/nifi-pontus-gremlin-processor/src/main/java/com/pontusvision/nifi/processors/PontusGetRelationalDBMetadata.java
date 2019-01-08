package com.pontusvision.nifi.processors;


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A processor to retrieve a list of tables (and their metadata) from a database connection
 */
@TriggerSerially @Tags({ "sql", "list", "jdbc", "table", "database", "pontus", "metadata" }) @CapabilityDescription(
    "Generates a set of flow files, each containing attributes corresponding to metadata about a table from a database connection. Once "
        + "metadata about a table has been fetched, it will not be fetched again until the Refresh Interval (if set) has elapsed, or until state has been "
        + "manually cleared.") @WritesAttributes({
    @WritesAttribute(attribute = "pg_rdb_table_name", description = "Contains the name of a database table from the connection"),
    @WritesAttribute(attribute = "pg_rdb_table_catalog", description = "Contains the name of the catalog to which the table belongs (may be null)"),
    @WritesAttribute(attribute = "pg_rdb_table_schema", description = "Contains the name of the schema to which the table belongs (may be null)"),
    @WritesAttribute(attribute = "pg_rdb_table_fullname", description = "Contains the fully-qualifed table name (possibly including catalog, schema, etc.)"),
    @WritesAttribute(attribute = "pg_rdb_col_metadata", description = "Contains a JSON representation of the colums metadata."),
    @WritesAttribute(attribute = "pg_rdb_table_type", description =
        "Contains the type of the database table from the connection. Typical types are \"TABLE\", \"VIEW\", \"SYSTEM TABLE\", "
            + "\"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\", \"ALIAS\", \"SYNONYM\""),
    @WritesAttribute(attribute = "pg_rdb_table_remarks", description = "Contains the name of a database table from the connection"),
    @WritesAttribute(attribute = "pg_rdb_table_count", description = "Contains the number of rows in the table") }) @Stateful(scopes = {
    Scope.CLUSTER }, description = "After performing a listing of tables, the timestamp of the query is stored. "
    + "This allows the Processor to not re-list tables the next time that the Processor is run. Specifying the refresh interval in the processor properties will "
    + "indicate that when the processor detects the interval has elapsed, the state will be reset and tables will be re-listed as a result. "
    + "This processor is meant to be run on the primary node only.")

public class PontusGetRelationalDBMetadata extends AbstractProcessor
{

  // Attribute names
  public static final String DB_TABLE_NAME     = "pg_rdb_table_name";
  public static final String DB_TABLE_CATALOG  = "pg_rdb_table_catalog";
  public static final String DB_TABLE_SCHEMA   = "pg_rdb_table_schema";
  public static final String DB_TABLE_FULLNAME = "pg_rdb_table_fullname";
  public static final String DB_TABLE_TYPE     = "pg_rdb_table_type";
  public static final String DB_TABLE_REMARKS  = "pg_rdb_table_remarks";
  public static final String DB_TABLE_COUNT    = "pg_rdb_table_count";
  public static final String DB_COL_METADATA   = "pg_rdb_col_metadata";

  // Relationships
  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
      .description("All FlowFiles that are received are routed to success").build();

  // Property descriptors
  public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
      .name("list-db-tables-db-connection").displayName("Database Connection Pooling Service")
      .description("The Controller Service that is used to obtain connection to database").required(true)
      .identifiesControllerService(DBCPService.class).build();

  public static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder().name("list-db-tables-catalog")
      .displayName("Catalog").description(
          "The name of a catalog from which to list database tables. The name must match the catalog name as it is stored in the database. "
              + "If the property is not set, the catalog name will not be used to narrow the search for tables. If the property is set to an empty string, "
              + "tables without a catalog will be listed.").required(false).addValidator(Validator.VALID).build();

  public static final PropertyDescriptor SCHEMA_PATTERN = new PropertyDescriptor.Builder()
      .name("list-db-tables-schema-pattern").displayName("Schema Pattern").description(
          "A pattern for matching schemas in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
              + "and \"_\" means match any one character. The pattern must match the schema name as it is stored in the database. "
              + "If the property is not set, the schema name will not be used to narrow the search for tables. If the property is set to an empty string, "
              + "tables without a schema will be listed.").required(false).addValidator(Validator.VALID).build();

  public static final PropertyDescriptor TABLE_NAME_PATTERN = new PropertyDescriptor.Builder()
      .name("list-db-tables-name-pattern").displayName("Table Name Pattern").description(
          "A pattern for matching tables in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
              + "and \"_\" means match any one character. The pattern must match the table name as it is stored in the database. "
              + "If the property is not set, all tables will be retrieved.").required(false)
      .addValidator(Validator.VALID).build();
  public static final PropertyDescriptor COLUMN_NAME_PATTERN = new PropertyDescriptor.Builder()
      .name("list-db-column-name-pattern").displayName("Column Name Pattern").description(
          "A pattern for matching columns in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
              + "and \"_\" means match any one character. The pattern must match the column name as it is stored in the database. "
              + "If the property is not set, all columns will be retrieved.").required(false)
      .addValidator(Validator.VALID).build();

  public static final PropertyDescriptor TABLE_TYPES = new PropertyDescriptor.Builder().name("list-db-tables-types")
      .displayName("Table Types").description(
          "A comma-separated list of table types to include. For example, some databases support TABLE and VIEW types. If the property is not set, "
              + "tables of all types will be returned.").required(false).defaultValue("TABLE")
      .addValidator(Validator.VALID).build();

  public static final PropertyDescriptor INCLUDE_COUNT = new PropertyDescriptor.Builder().name("list-db-include-count")
      .displayName("Include Count").description(
          "Whether to include the table's row count as a flow file attribute. This affects performance as a database query will be generated "
              + "for each table in the retrieved list.").required(true).allowableValues("true", "false")
      .defaultValue("false").build();

  public static final PropertyDescriptor NUM_ROWS = new PropertyDescriptor.Builder().name("list-db-num-rows")
      .displayName("Num Rows").description(
          "The number of rows to retrieve from each table.  Setting this to 0 does not retrieve any data.  Note that this can cause performance issues in large tables")
      .required(true).addValidator(StandardValidators.NUMBER_VALIDATOR).defaultValue("0").build();

  public static final PropertyDescriptor REFRESH_INTERVAL = new PropertyDescriptor.Builder()
      .name("list-db-refresh-interval").displayName("Refresh Interval").description(
          "The amount of time to elapse before resetting the processor state, thereby causing all current tables to be listed. "
              + "During this interval, the processor may continue to run, but tables that have already been listed will not be re-listed. However new/added "
              + "tables will be listed as the processor runs. A value of zero means the state will never be automatically reset, the user must "
              + "Clear State manually.").required(true).defaultValue("0 sec")
      .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

  private static final List<PropertyDescriptor> propertyDescriptors;
  private static final Set<Relationship> relationships;

  /*
   * Will ensure that the list of property descriptors is build only once.
   * Will also create a Set of relationships
   */
  static
  {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.add(DBCP_SERVICE);
    _propertyDescriptors.add(CATALOG);
    _propertyDescriptors.add(SCHEMA_PATTERN);
    _propertyDescriptors.add(TABLE_NAME_PATTERN);
    _propertyDescriptors.add(COLUMN_NAME_PATTERN);

    _propertyDescriptors.add(TABLE_TYPES);
    _propertyDescriptors.add(INCLUDE_COUNT);
    _propertyDescriptors.add(REFRESH_INTERVAL);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_SUCCESS);
    relationships = Collections.unmodifiableSet(_relationships);
  }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return propertyDescriptors;
  }

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException
  {
    final ComponentLog logger = getLogger();
    final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    final String catalog = context.getProperty(CATALOG).getValue();
    final String schemaPattern = context.getProperty(SCHEMA_PATTERN).getValue();
    final String tableNamePattern = context.getProperty(TABLE_NAME_PATTERN).getValue();

    final String columnNamePattern = context.getProperty(COLUMN_NAME_PATTERN).getValue();
    final String[] tableTypes = context.getProperty(TABLE_TYPES).isSet() ?
        context.getProperty(TABLE_TYPES).getValue().split("\\s*,\\s*") :
        null;
    final boolean includeCount = context.getProperty(INCLUDE_COUNT).asBoolean();
    final int numRows = context.getProperty(NUM_ROWS).asInteger();
    final long refreshInterval = context.getProperty(REFRESH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

    final StateManager stateManager = context.getStateManager();
    final StateMap stateMap;
    final Map<String, String> stateMapProperties;
    try
    {
      stateMap = stateManager.getState(Scope.CLUSTER);
      stateMapProperties = new HashMap<>(stateMap.toMap());
    }
    catch (IOException ioe)
    {
      throw new ProcessException(ioe);
    }

    try (final Connection con = dbcpService.getConnection())
    {

      DatabaseMetaData dbMetaData = con.getMetaData();
      ResultSet rs = dbMetaData.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
      while (rs.next())
      {
        final String tableCatalog = rs.getString(1);
        final String tableSchema = rs.getString(2);
        final String tableName = rs.getString(3);
        final String tableType = rs.getString(4);
        final String tableRemarks = rs.getString(5);

        // Build fully-qualified name
        String fqn = Stream.of(tableCatalog, tableSchema, tableName).filter(segment -> !StringUtils.isEmpty(segment))
            .collect(Collectors.joining("."));

        String lastTimestampForTable = stateMapProperties.get(fqn);
        boolean refreshTable = true;
        try
        {
          // Refresh state if the interval has elapsed
          long lastRefreshed = -1;
          final long currentTime = System.currentTimeMillis();
          if (!StringUtils.isEmpty(lastTimestampForTable))
          {
            lastRefreshed = Long.parseLong(lastTimestampForTable);
          }
          if (lastRefreshed == -1 || (refreshInterval > 0 && currentTime >= (lastRefreshed + refreshInterval)))
          {
            stateMapProperties.remove(lastTimestampForTable);
          }
          else
          {
            refreshTable = false;
          }
        }
        catch (final NumberFormatException nfe)
        {
          getLogger().error("Failed to retrieve observed last table fetches from the State Manager. Will not perform "
              + "query until this is accomplished.", nfe);
          context.yield();
          return;
        }
        if (refreshTable)
        {
          ResultSet primaryKeysRs = dbMetaData.getPrimaryKeys(catalog, tableSchema, tableName);
          /*
           * <P>Each primary key column description has the following columns:
           *  <OL>
           *  <LI><B>(1)TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
           *  <LI><B>(2)TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
           *  <LI><B>(3)TABLE_NAME</B> String {@code =>} table name
           *  <LI><B>(4)COLUMN_NAME</B> String {@code =>} column name
           *  <LI><B>(5)KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
           *  of 1 represents the first column of the primary key, a value of 2 would
           *  represent the second column within the primary key).
           *  <LI><B>(6)PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
           *  </OL>
           */

          Map<String, String> primaryKeys = new HashMap<>();

          while (primaryKeysRs.next())
          {
            final String colName = primaryKeysRs.getString(4);
            final String pkName = primaryKeysRs.getString(6);
            primaryKeys.put(colName, pkName);
          }

          ResultSet foreignKeysRs = dbMetaData.getImportedKeys(catalog, tableSchema, tableName);
          /*
           * <P>Each primary key column description has the following columns:
           *  <OL>
           *  <LI><B>(1)PKTABLE_CAT</B> String {@code =>} primary key table catalog
           *      being imported (may be <code>null</code>)
           *  <LI><B>(2)PKTABLE_SCHEM</B> String {@code =>} primary key table schema
           *      being imported (may be <code>null</code>)
           *  <LI><B>(3)PKTABLE_NAME</B> String {@code =>} primary key table name
           *      being imported
           *  <LI><B>(4)PKCOLUMN_NAME</B> String {@code =>} primary key column name
           *      being imported
           *  <LI><B>(5)FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
           *  <LI><B>(6)FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>)
           *  <LI><B>(7)FKTABLE_NAME</B> String {@code =>} foreign key table name
           *  <LI><B>(8)FKCOLUMN_NAME</B> String {@code =>} foreign key column name
           *  <LI><B>(1)KEY_SEQ</B> short {@code =>} sequence number within a foreign key( a value
           *  of 1 represents the first column of the foreign key, a value of 2 would
           *  represent the second column within the foreign key).
           *  <LI><B>(1)UPDATE_RULE</B> short {@code =>} What happens to a
           *       foreign key when the primary key is updated:
           *      <UL>
           *      <LI> importedNoAction - do not allow update of primary
           *               key if it has been imported
           *      <LI> importedKeyCascade - change imported key to agree
           *               with primary key update
           *      <LI> importedKeySetNull - change imported key to <code>NULL</code>
           *               if its primary key has been updated
           *      <LI> importedKeySetDefault - change imported key to default values
           *               if its primary key has been updated
           *      <LI> importedKeyRestrict - same as importedKeyNoAction
           *                                 (for ODBC 2.x compatibility)
           *      </UL>
           *  <LI><B>(1)DELETE_RULE</B> short {@code =>} What happens to
           *      the foreign key when primary is deleted.
           *      <UL>
           *      <LI> importedKeyNoAction - do not allow delete of primary
           *               key if it has been imported
           *      <LI> importedKeyCascade - delete rows that import a deleted key
           *      <LI> importedKeySetNull - change imported key to NULL if
           *               its primary key has been deleted
           *      <LI> importedKeyRestrict - same as importedKeyNoAction
           *                                 (for ODBC 2.x compatibility)
           *      <LI> importedKeySetDefault - change imported key to default if
           *               its primary key has been deleted
           *      </UL>
           *  <LI><B>(1)FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
           *  <LI><B>(1)PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
           *  <LI><B>(1)DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
           *      constraints be deferred until commit
           *      <UL>
           *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
           *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
           *      <LI> importedKeyNotDeferrable - see SQL92 for definition
           *      </UL>
           */

          Map<String, String> foreignKeys = new HashMap<>();

          while (foreignKeysRs.next())
          {
            final String colName = foreignKeysRs.getString(4);
            final String fkTableName = foreignKeysRs.getString(7);
            final String fkColName = foreignKeysRs.getString(8);

            String val = Stream.of(fkTableName, fkColName).filter(segment -> !StringUtils.isEmpty(segment))
                .collect(Collectors.joining("."));

            primaryKeys.put(colName, val);
          }
          FlowFile flowFile = session.create();

          if (includeCount)
          {
            try (Statement st = con.createStatement())
            {
              final String countQuery = "SELECT COUNT(1) FROM " + fqn;

              logger.debug("Executing query: {}", new Object[] { countQuery });
              ResultSet countResult = st.executeQuery(countQuery);
              if (countResult.next())
              {
                flowFile = session.putAttribute(flowFile, DB_TABLE_COUNT, Long.toString(countResult.getLong(1)));
              }
            }
            catch (SQLException se)
            {
              logger.error("Couldn't get row count for {}", new Object[] { fqn });
              session.remove(flowFile);
              continue;
            }
          }
          Map<String, JsonArrayBuilder> sampleRows = new HashMap<>();

          if (numRows > 0)
          {
            try (Statement st = con.createStatement())
            {
              final String countQuery = "SELECT * FROM " + fqn + " LIMIT " + numRows;

              logger.debug("Executing query: {}", new Object[] { countQuery });
              ResultSet rowsResult = st.executeQuery(countQuery);

              while (rowsResult.next())
              {
                ResultSetMetaData metaData = rowsResult.getMetaData();
                int colCount = metaData.getColumnCount();

                for (int i = 1; i <= colCount; i++)
                {
                  final String colName = metaData.getColumnName(i);
                  String val = rowsResult.getObject(i).toString();
                  JsonArrayBuilder vals = sampleRows.putIfAbsent(colName, Json.createArrayBuilder());
                  vals.add(val);
                }
                flowFile = session.putAttribute(flowFile, DB_TABLE_COUNT, Long.toString(rowsResult.getLong(1)));
              }
            }
            catch (SQLException se)
            {
              logger.error("Couldn't get row count for {}", new Object[] { fqn });
              session.remove(flowFile);
              continue;
            }

          }

          ResultSet colsRs = dbMetaData.getColumns(catalog, tableSchema, tableName, columnNamePattern);


          /*
           * <P>Each column description has the following columns:
           *  <OL>
           *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
           *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
           *  <LI><B>TABLE_NAME</B> String {@code =>} table name
           *  <LI><B>(4)COLUMN_NAME</B> String {@code =>} column name
           *  <LI><B>(5)DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
           *  <LI><B>(6)TYPE_NAME</B> String {@code =>} Data source dependent type name,
           *  for a UDT the type name is fully qualified
           *  <LI><B>(7)COLUMN_SIZE</B> int {@code =>} column size.
           *  <LI><B>(8)BUFFER_LENGTH</B> is not used.
           *  <LI><B>(9)DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
           * DECIMAL_DIGITS is not applicable.
           *  <LI><B>(10)NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
           *  <LI><B>(11)NULLABLE</B> int {@code =>} is NULL allowed.
           *      <UL>
           *      <LI> columnNoNulls - might not allow <code>NULL</code> values
           *      <LI> columnNullable - definitely allows <code>NULL</code> values
           *      <LI> columnNullableUnknown - nullability unknown
           *      </UL>
           *  <LI><B>(12)REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
           *  <LI><B>(13)COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
           *  <LI><B>(14)SQL_DATA_TYPE</B> int {@code =>} unused
           *  <LI><B>(15)SQL_DATETIME_SUB</B> int {@code =>} unused
           *  <LI><B>(16)CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
           *       maximum number of bytes in the column
           *  <LI><B>(17)ORDINAL_POSITION</B> int {@code =>} index of column in table
           *      (starting at 1)
           *  <LI><B>(18)IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
           *       <UL>
           *       <LI> YES           --- if the column can include NULLs
           *       <LI> NO            --- if the column cannot include NULLs
           *       <LI> empty string  --- if the nullability for the
           * column is unknown
           *       </UL>
           *  <LI><B>(19)SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
           *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
           *  <LI><B>(20)SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
           *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
           *  <LI><B>(21)SCOPE_TABLE</B> String {@code =>} table name that this the scope
           *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
           *  <LI><B>(22)SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
           *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
           *      isn't DISTINCT or user-generated REF)
           *   <LI><B>(23)IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
           *       <UL>
           *       <LI> YES           --- if the column is auto incremented
           *       <LI> NO            --- if the column is not auto incremented
           *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
           *       </UL>
           *   <LI><B>(24)IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
           *       <UL>
           *       <LI> YES           --- if this a generated column
           *       <LI> NO            --- if this not a generated column
           *       <LI> empty string  --- if it cannot be determined whether this is a generated column
           *       </UL>
           *  </OL>
           *
           * <p>The COLUMN_SIZE column specifies the column size for the given column.
           * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
           * For datetime datatypes, this is the length in characters of the String representation (assuming the
           * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
           * this is the length in bytes. Null is returned for data types where the
           * column size is not applicable.
           *
           */

          JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

          JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
          jsonBuilder.add("colMetaData", jsonArrayBuilder);

          while (colsRs.next())
          {
            JsonObjectBuilder colDetail = Json.createObjectBuilder();

            final String colName = colsRs.getString(4);

            //            final boolean isPrimaryKey = primaryKeys.containsKey(colName);
            final String primaryKeyName = primaryKeys.get(colName);
            final String foreignKeyName = foreignKeys.get(colName);

//            final int dataType = colsRs.getInt(5);
            final String typeName = rs.getString(6);
            final int colSize = colsRs.getInt(7);
            final String colRemarks = colsRs.getString(12);
            final String defVal = colsRs.getString(13);
            final int octetLen = colsRs.getInt(16);
            final int ordinalPos = colsRs.getInt(17);
            final String isAutoIncr = colsRs.getString(23);
            final String isGenerated = colsRs.getString(24);
            final JsonArrayBuilder vals = numRows == 0 ? null : sampleRows.get(colName);

            colDetail.add("colName", colName)
                .add("primaryKeyName", primaryKeyName)
                .add("foreignKeyName",foreignKeyName)
                .add("typeName",typeName)
                .add("colRemarks",colRemarks)
                .add("isAutoIncr",isAutoIncr)
                .add("isGenerated",isGenerated)
                .add("octetLen",octetLen)
                .add("ordinalPos",ordinalPos)
                .add("defVal",defVal)
                .add("colSize",colSize)
                .add("vals",vals);
            jsonArrayBuilder.add(colDetail);

            logger.info("Found {}: {}", new Object[] { tableType, fqn });
          }

          if (tableCatalog != null)
          {
            jsonBuilder.add("tableCatalog",tableCatalog);
            flowFile = session.putAttribute(flowFile, DB_TABLE_CATALOG, tableCatalog);
          }
          if (tableSchema != null)
          {
            jsonBuilder.add("tableSchema",tableSchema);

            flowFile = session.putAttribute(flowFile, DB_TABLE_SCHEMA, tableSchema);
          }
          jsonBuilder.add("tableName",tableSchema);
          jsonBuilder.add("fqn",fqn);
          jsonBuilder.add("tableType",tableType);

          flowFile = session.putAttribute(flowFile, DB_TABLE_NAME, tableName);
          flowFile = session.putAttribute(flowFile, DB_TABLE_FULLNAME, fqn);
          flowFile = session.putAttribute(flowFile, DB_TABLE_TYPE, tableType);
          if (tableRemarks != null)
          {
            jsonBuilder.add("tableRemarks",tableRemarks);

            flowFile = session.putAttribute(flowFile, DB_TABLE_REMARKS, tableRemarks);

          }


          flowFile = session.putAttribute(flowFile, DB_COL_METADATA, jsonBuilder.toString());

          String transitUri;
          try
          {
            transitUri = dbMetaData.getURL();
          }
          catch (SQLException sqle)
          {
            transitUri = "<unknown>";
          }
                  session.getProvenanceReporter().receive(flowFile, transitUri);
          session.transfer(flowFile, REL_SUCCESS);

          stateMapProperties.put(fqn, Long.toString(System.currentTimeMillis()));
        }
      }
      // Update the timestamps for listed tables
      if (stateMap.getVersion() == -1)
      {
        stateManager.setState(stateMapProperties, Scope.CLUSTER);
      }
      else
      {
        stateManager.replace(stateMap, stateMapProperties, Scope.CLUSTER);
      }

    }
    catch (final SQLException | IOException e)
    {
      throw new ProcessException(e);
    }
  }

}
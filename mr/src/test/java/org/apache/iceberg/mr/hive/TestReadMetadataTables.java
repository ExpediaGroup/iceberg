/*
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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestReadMetadataTables {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private TestTables testTables;

  private static final Schema CUSTOMER_SCHEMA = new Schema(
      required(1, "customer_id", Types.LongType.get()),
      required(2, "first_name", Types.StringType.get())
  );

  private static final List<Record> CUSTOMER_RECORDS = TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
      .add(0L, "Alice")
      .add(1L, "Bob")
      .add(2L, "Trudy")
      .build();

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static TestHiveShell shell;

  @BeforeClass
  public static void beforeClass() {
    shell = new TestHiveShell();
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
    shell.setHiveConfValue("hive.tez.exec.print.summary", "true");
    shell.start();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    shell.openSession();
    testTables = new TestTables.HadoopTestTables(shell.metastore().hiveConf(), temp);
    //for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
    //  shell.setHiveSessionValue(property.getKey(), property.getValue());
    //}
    //TODO: do we want to test this with tez and mr as params?
    //shell.setHiveSessionValue("hive.execution.engine", executionEngine);
    shell.setHiveSessionValue("hive.jar.directory", temp.getRoot().getAbsolutePath());
    shell.setHiveSessionValue("tez.staging-dir", temp.getRoot().getAbsolutePath());
  }

  @After
  public void after() throws Exception {
    shell.closeSession();
    shell.metastore().reset();
    // HiveServer2 thread pools are using thread local Hive -> HMSClient objects. These are not cleaned up when the
    // HiveServer2 is stopped. Only Finalizer closes the HMS connections.
    System.gc();
  }

  @Test
  public void bla() throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, rows.get(2));
  }

  @Test
  public void testReadSnapshotTable() throws IOException {
    Table table = createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);
    System.out.println("XXX: " + table.location());
    shell.executeStatement(new StringBuilder()
        .append("CREATE TABLE default.customer_snapshots ")
        .append("STORED BY '").append(HiveIcebergStorageHandler.class.getName()).append("' ")
        .append("LOCATION '")
        .append(table.location() + "/default/customers#snapshots'")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM default.customer_snapshots");

    Assert.assertEquals(3, result.size());
  }

  private Table createTable(String tableName, Schema schema, List<Record> records)
      throws IOException {
    Table table = createIcebergTable(tableName, schema, records);
    createHiveTable(tableName, table.location());
    return table;
  }

  protected Table createIcebergTable(String tableName, Schema schema, List<Record> records)
      throws IOException {
    String identifier = testTables.identifier("default." + tableName);
    TestHelper helper = new TestHelper(
        shell.metastore().hiveConf(), testTables.tables(), identifier, schema, SPEC, FileFormat.PARQUET, temp);
    Table table = helper.createTable();

    if (!records.isEmpty()) {
      helper.appendToTable(helper.writeFile(null, records));
    }

    return table;
  }

  protected void createHiveTable(String tableName, String location) {
    shell.executeStatement(String.format(
        "CREATE TABLE default.%s " +
            "STORED BY '%s' " +
            "LOCATION '%s'",
        tableName, HiveIcebergStorageHandler.class.getName(), location));
  }
  /*


  private File tableLocation;
  private Configuration conf = new Configuration();
  private HadoopCatalog catalog;
  private Schema schema = new Schema(required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));
  private long snapshotId;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    catalog = new HadoopCatalog(conf, tableLocation.getAbsolutePath());
    PartitionSpec spec = PartitionSpec.unpartitioned();

    TableIdentifier id = TableIdentifier.parse("source_db.table_a");
    Table table = catalog.createTable(id, schema, spec);

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    data.add(TestHelpers.createSimpleRecord(2L, "Andy"));
    data.add(TestHelpers.createSimpleRecord(3L, "Berta"));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    DataFile fileB = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    DataFile fileC = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileB).commit();
    table.newAppend().appendFile(fileC).commit();

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    snapshotId = snapshots.get(0).snapshotId();
  }

  @Test
  public void testReadHistoryTable() {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath() + "/source_db/table_a#history'")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a");

    assertEquals(3, result.size());
  }

  @Ignore("TODO: re-enable this test when snapshot functionality added")
  @Test
  public void testCreateRegularTableEndingWithSnapshots() throws IOException {
    TableIdentifier id = TableIdentifier.parse("source_db.table_a__snapshots");
    Table table = catalog.createTable(id, schema, PartitionSpec.unpartitioned());

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();

    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a__snapshots")
        .append("' TBLPROPERTIES ('iceberg.snapshots.table'='false')")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a__snapshots");

    assertEquals(1, result.size());
  }

  @Ignore("TODO: re-enable this test when snapshot functionality added")
  @Test
  public void testTimeTravelRead() {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a'")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a#snapshots'")
        .toString());

    List<Object[]> resultLatestTable = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(9, resultLatestTable.size());

    List<Object[]> resultFirstSnapshot = shell.executeStatement(
        "SELECT * FROM source_db.table_a WHERE SNAPSHOT__ID = " + snapshotId);
    assertEquals(3, resultFirstSnapshot.size());

    List<Object[]> resultLatestSnapshotAgain = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(9, resultLatestSnapshotAgain.size());
  }

  @Test
  public void testCreateTableWithSnapshotIDColumnInSchema() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    schema = new Schema(required(1, "snapshot__id", Types.LongType.get()),
        optional(2, "data", Types.StringType.get()));
    TableIdentifier id = TableIdentifier.parse("source_db.table_b");
    Table table = catalog.createTable(id, schema, spec);

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();

    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_b ")
        .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_b")
        .append("' TBLPROPERTIES (")
        .append("'iceberg.hive.snapshot.virtual.column.name' = 'metadata_snapshot_id')")
        .toString());

    List<Object[]> resultLatestTable = shell.executeStatement("SELECT * FROM source_db.table_b");
    assertEquals(1, resultLatestTable.size());
  }

  */
}

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

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;

public class TestReadMetadataTables {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private TestTables testTables;
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
    //TODO: sort out if we need test tables...
    //testTables = testTables(shell.metastore().hiveConf(), temp);
    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveSessionValue(property.getKey(), property.getValue());
    }
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
  public void testReadSnapshotTable() {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            .append("STORED BY 'org.apache.iceberg.mr.mapred.IcebergStorageHandler' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath() + "/source_db/table_a#snapshots'")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a");

    assertEquals(3, result.size());
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



  @Ignore("TODO: re-enable this test when snapshot functionality added")
  @Test
  public void testAllRowsIncludeSnapshotId() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("ROW FORMAT SERDE 'org.apache.iceberg.mr.mapred.IcebergSerDe' ")
        .append("STORED AS ")
        .append("INPUTFORMAT 'org.apache.iceberg.mr.mapred.IcebergInputFormat' ")
        .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("'")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a");

    assertEquals(4, result.size());
    assertEquals(snapshotId, result.get(0)[2]);
    assertEquals(snapshotId, result.get(1)[2]);
    assertEquals(snapshotId, result.get(2)[2]);
    assertEquals(snapshotId, result.get(3)[2]);
  }
  */
}

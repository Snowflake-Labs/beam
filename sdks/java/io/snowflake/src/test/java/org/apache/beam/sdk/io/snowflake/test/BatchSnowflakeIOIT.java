/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.test;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.SnowflakeIOITPipelineOptions;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowCsvMapper;
import static org.apache.beam.sdk.io.snowflake.test.TestUtils.getTestRowDataMapper;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test of {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO} on an independent Snowflake
 * instance.
 *
 * <p>This test requires a running instance of Snowflake, configured for your GCP account. Pass in
 * connection information using PipelineOptions:
 *
 * <pre>
 * ./gradlew -p sdks/java/io/snowflake integrationTest -DintegrationTestPipelineOptions='[
 * "--serverName=<YOUR SNOWFLAKE SERVER NAME>",
 * "--username=<USERNAME>",
 * "--password=<PASSWORD>",
 * "--database=<DATABASE NAME>",
 * "--role=<SNOWFLAKE ROLE>",
 * "--warehouse=<SNOWFLAKE WAREHOUSE NAME>",
 * "--schema=<SCHEMA NAME>",
 * "--stagingBucketName=gs://<GCS BUCKET NAME>",
 * "--storageIntegrationName=<STORAGE INTEGRATION NAME>",
 * "--numberOfRecords=<1000, 100000, 600000, 5000000>",
 * "--runner=DataflowRunner",
 * "--region=<GCP REGION FOR DATAFLOW RUNNER>",
 * "--project=<GCP_PROJECT>"]' \
 * --tests org.apache.beam.sdk.io.snowflake.test.BatchSnowflakeIOIT \
 * -DintegrationTestRunner=dataflow
 * </pre>
 */
public class BatchSnowflakeIOIT {
  private static final String tableName = "IOIT";

  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static int numberOfRecords;
  private static String stagingBucketName;
  private static String storageIntegrationName;
  private static SnowflakeTableSchema tableSchema;

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() throws SQLException {
    SnowflakeIOITPipelineOptions options =
        readIOTestPipelineOptions(SnowflakeIOITPipelineOptions.class);

    numberOfRecords = options.getNumberOfRecords();
    stagingBucketName = options.getStagingBucketName();
    storageIntegrationName = options.getStorageIntegrationName();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create()
            .withUsernamePasswordAuth(options.getUsername(), options.getPassword())
            .withKeyPairPathAuth(
                options.getUsername(),
                options.getPrivateKeyPath(),
                options.getPrivateKeyPassphrase())
            .withDatabase(options.getDatabase())
            .withRole(options.getRole())
            .withWarehouse(options.getWarehouse())
            .withServerName(options.getServerName())
            .withSchema(options.getSchema());

    tableSchema =
        SnowflakeTableSchema.of(
            SnowflakeColumn.of("id", SnowflakeInteger.of()),
            SnowflakeColumn.of("name", SnowflakeString.of()));
  }

  @Test
  public void testWriteTestRowThenRead() {
    // Write data to Snowflake
    pipelineWrite
        .apply(GenerateSequence.from(0).to(numberOfRecords))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(getTestRowDataMapper())
                .to(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(tableSchema));

    PipelineResult writeResult = pipelineWrite.run();
    writeResult.waitUntilFinish();

    // Read data from table
    PCollection<TestRow> namesAndIds =
        pipelineRead.apply(
            SnowflakeIO.<TestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCsvMapper(getTestRowCsvMapper())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRecords);

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRecords));

    PipelineResult readResult = pipelineRead.run();
    readResult.waitUntilFinish();
  }

  @AutoValue
  abstract static class NullableTestRow implements Serializable, Comparable<NullableTestRow> {
    /** Manually create a test row. */
    public static NullableTestRow create(Integer id, String name) {
      return new AutoValue_BatchSnowflakeIOIT_NullableTestRow(id, name);
    }

    public abstract Integer id();

    @Nullable
    public abstract String name();

    @Override
    public int compareTo(@NotNull BatchSnowflakeIOIT.NullableTestRow o) {
      return Integer.compare(this.id(), o.id());
    }
    /** Outputs just the name stored in the {@link NullableTestRow}. */
    public static class SelectNameFn extends DoFn<NullableTestRow, String> {
      @DoFn.ProcessElement
      public void processElement(ProcessContext c) {
        c.output(c.element().name());
      }
    }
  }

  @Test
  public void testWriteVariousTypesThenRead() throws Exception {
    // Write data to Snowflake
    Set<NullableTestRow> inputRows =
        new HashSet<>(
            Arrays.asList(
                NullableTestRow.create(0, null),
                NullableTestRow.create(1, "TEST"),
                NullableTestRow.create(2, "")));

    pipelineWrite
        .apply(Create.of(inputRows))
        .apply(
            SnowflakeIO.<NullableTestRow>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withWriteDisposition(WriteDisposition.TRUNCATE)
                .withUserDataMapper(
                    (NullableTestRow element) -> new Object[] {element.id(), element.name()})
                .to(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withTableSchema(tableSchema));

    PipelineResult writeResult = pipelineWrite.run();
    writeResult.waitUntilFinish();

    Assert.assertEquals(readNullableTestRowFromTable(), inputRows);

    // Read data from table
    PCollection<NullableTestRow> outputRows =
        pipelineRead.apply(
            SnowflakeIO.<NullableTestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(tableName)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withCsvMapper(parts -> NullableTestRow.create(Integer.valueOf(parts[0]), parts[1]))
                .withCoder(SerializableCoder.of(NullableTestRow.class)));

    PAssert.that(outputRows).containsInAnyOrder(inputRows);

    PipelineResult readResult = pipelineRead.run();
    readResult.waitUntilFinish();
  }

  @AfterClass
  public static void teardown() throws Exception {
    String combinedPath = stagingBucketName + "/**";
    List<ResourceId> paths =
        FileSystems.match(combinedPath).metadata().stream()
            .map(MatchResult.Metadata::resourceId)
            .collect(Collectors.toList());

    FileSystems.delete(paths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

    TestUtils.runConnectionWithStatement(
        dataSourceConfiguration.buildDatasource(), String.format("DROP TABLE %s", tableName));
  }

  private Set<NullableTestRow> readNullableTestRowFromTable() throws SQLException {
    Connection connection = dataSourceConfiguration.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(String.format("SELECT * FROM %s", tableName));
    ResultSet resultSet = statement.executeQuery();

    Set<NullableTestRow> testRows = resultSetToJavaSet(resultSet);

    resultSet.close();
    statement.close();
    connection.close();

    return testRows;
  }

  private Set<NullableTestRow> resultSetToJavaSet(ResultSet resultSet) throws SQLException {
    Set<NullableTestRow> testRows = new HashSet<>();
    while (resultSet.next()) {
      int rowId = resultSet.getInt(1);
      String name = resultSet.getString(2);
      testRows.add(NullableTestRow.create(rowId, name));
      System.out.printf("rowId=%s, name=%s%n", rowId, name);
    }
    return testRows;
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.index.loader.defaultcolumn;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.SegmentMetadataImplTest;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.core.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction.ADD_DIMENSION;
import static org.apache.pinot.core.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction.ADD_METRIC;
import static org.apache.pinot.core.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction.REMOVE_DIMENSION;
import static org.apache.pinot.core.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction.UPDATE_DIMENSION;
import static org.apache.pinot.core.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.computeDefaultColumnActionMap;


public class BaseDefaultColumnHandlerTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private File INDEX_DIR;
  private File segmentDirectory;
  private SegmentMetadataImpl segmentMetadata;

  @BeforeMethod
  public void setUp()
      throws Exception {
    INDEX_DIR = Files.createTempDirectory(SegmentMetadataImplTest.class.getName() + "_segmentDir").toFile();

    final String filePath =
        TestUtils.getFileFromResourceUrl(SegmentMetadataImplTest.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    config.setCheckTimeColumnValidityDuringGeneration(false);
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(segmentDirectory);
  }

  @Test
  public void testComputeDefaultColumnActionMapForCommittedSegment() {
    // Same schema
    Schema schema0 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema0, segmentMetadata), Collections.EMPTY_MAP);

    // Add single-value dimension in the schema
    Schema schema1 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column11", FieldSpec.DataType.INT)  // add column11
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema1, segmentMetadata),
        ImmutableMap.of("column11", ADD_DIMENSION));

    // Add multi-value dimension in the schema
    Schema schema2 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addMultiValueDimension("column11", FieldSpec.DataType.INT) // add column11
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema2, segmentMetadata),
        ImmutableMap.of("column11", ADD_DIMENSION));

    // Add metric in the schema
    Schema schema3 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT)
            .addMetric("column11", FieldSpec.DataType.INT).build(); // add column11
    Assert
        .assertEquals(computeDefaultColumnActionMap(schema3, segmentMetadata), ImmutableMap.of("column11", ADD_METRIC));

    // Do not remove non-autogenerated column in the segmentMetadata
    Schema schema4 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)  // remove column2
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema4, segmentMetadata), Collections.EMPTY_MAP);

    // Do not update non-autogenerated column in the schema
    Schema schema5 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.STRING)  // update datatype
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addSingleValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema5, segmentMetadata), Collections.EMPTY_MAP);
  }

  @Test
  public void testComputeDefaultColumnActionMapForConsumingSegment() {
    segmentMetadata.setColumnMetadataMap(null);

    // Same schema
    Schema schema0 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema0, segmentMetadata), Collections.EMPTY_MAP);

    // Add single-value dimension in the schema
    Schema schema1 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column11", FieldSpec.DataType.INT)  // add column11
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema1, segmentMetadata),
        ImmutableMap.of("column11", ADD_DIMENSION));

    // Add multi-value dimension in the schema
    Schema schema2 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addMultiValueDimension("column11", FieldSpec.DataType.INT) // add column11
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema2, segmentMetadata),
        ImmutableMap.of("column11", ADD_DIMENSION));

    // Add metric in the schema
    Schema schema3 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT)
            .addMetric("column11", FieldSpec.DataType.INT).build(); // add column11
    Assert
        .assertEquals(computeDefaultColumnActionMap(schema3, segmentMetadata), ImmutableMap.of("column11", ADD_METRIC));

    // Remove column in the segmentMetadata
    Schema schema4 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)  // remove column2
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema4, segmentMetadata),
        ImmutableMap.of("column2", REMOVE_DIMENSION));

    // Update column type in the schema
    Schema schema5 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.STRING)  // update datatype
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addMultiValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema5, segmentMetadata),
        ImmutableMap.of("column2", UPDATE_DIMENSION));

    // Update column from single-value to multi-value in the schema
    Schema schema6 =
        new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("column1", FieldSpec.DataType.INT)
            .addSingleValueDimension("column2", FieldSpec.DataType.INT)
            .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
            .addSingleValueDimension("column6", FieldSpec.DataType.INT)
            .addMultiValueDimension("column7", FieldSpec.DataType.INT)
            .addSingleValueDimension("column8", FieldSpec.DataType.INT)
            .addSingleValueDimension("column9", FieldSpec.DataType.INT)
            .addSingleValueDimension("column10", FieldSpec.DataType.INT)
            .addSingleValueDimension("column13", FieldSpec.DataType.INT)
            .addSingleValueDimension("count", FieldSpec.DataType.INT)
            .addSingleValueDimension("daysSinceEpoch", FieldSpec.DataType.INT)
            .addSingleValueDimension("weeksSinceEpochSunday", FieldSpec.DataType.INT).build();
    Assert.assertEquals(computeDefaultColumnActionMap(schema6, segmentMetadata),
        ImmutableMap.of("column6", UPDATE_DIMENSION));
  }
}
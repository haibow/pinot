package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilsTest {

  String AVRO_SCHEMA = "fake_avro_schema.avsc";

  @Test
  public void testGetPinotSchemaFromAvroSchemaNullFieldTypeMap()
      throws IOException {
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, null, null);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addSingleValueDimension("m1", DataType.INT).addSingleValueDimension("m2", DataType.INT)
        .addSingleValueDimension("hoursSinceEpoch", DataType.LONG).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);
  }

  @Test
  public void testGetPinotSchemaFromAvroSchemaWithFieldTypeMap()
      throws IOException {
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    Map<String, FieldSpec.FieldType> fieldSpecMap =
        ImmutableMap.of("hoursSinceEpoch", FieldType.TIME, "m1", FieldType.METRIC, "m2", FieldType.METRIC);
    Schema inferredPinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, fieldSpecMap, TimeUnit.HOURS);
    Schema expectedSchema = new Schema.SchemaBuilder().addSingleValueDimension("d1", DataType.STRING)
        .addSingleValueDimension("d2", DataType.LONG).addSingleValueDimension("d3", DataType.STRING)
        .addMetric("m1", DataType.INT).addMetric("m2", DataType.INT)
        .addTime("hoursSinceEpoch", TimeUnit.HOURS, DataType.LONG).build();
    Assert.assertEquals(expectedSchema, inferredPinotSchema);
  }
}

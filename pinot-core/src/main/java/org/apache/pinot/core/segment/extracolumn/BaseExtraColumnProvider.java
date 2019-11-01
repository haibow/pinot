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
package org.apache.pinot.core.segment.extracolumn;

import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.readers.IntSingleValueDataFileReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Shared implementation code between extra column providers.
 */
public abstract class BaseExtraColumnProvider implements ExtraColumnProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExtraColumnProvider.class);

  protected ColumnMetadata.Builder getColumnMetadataBuilder(FieldSpec fieldSpec) {

    return new ColumnMetadata.Builder().setVirtual(fieldSpec.isVirtualColumn()).setColumnName(fieldSpec.getName())
        .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setFieldType(fieldSpec.getFieldType())
        .setDataType(fieldSpec.getDataType()).setSingleValue(fieldSpec.isSingleValueField()).setIsSorted(true);
  }

  public DataFileReader buildReader(ExtraColumnContext context) {
    if (context.getFieldSpec().isSingleValueField())
      return new IntSingleValueDataFileReader(0);
    else
      return new UniqueMultiValueInvertedIndex(0);
  }

  public ColumnIndexContainer buildColumnIndexContainer(ExtraColumnContext context) {
    return new ExtraColumnIndexContainer(buildReader(context), buildInvertedIndex(context), buildDictionary(context));
  }

  public ColumnMetadata buildMetadata(ExtraColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = getColumnMetadataBuilder(context.getFieldSpec());
    return columnMetadataBuilder.build();
  }

  public InvertedIndexReader buildInvertedIndex(ExtraColumnContext context) {
    LOGGER
        .debug("fieldSpec: " + context.getFieldSpec().toString() + ", " + context.getFieldSpec().isSingleValueField());
    if (context.getFieldSpec().isSingleValueField()) {
      return new UniqueSingleValueInvertedIndex(context.getTotalDocCount());
    } else {
      return new UniqueMultiValueInvertedIndex(context.getTotalDocCount());
    }
  }
}

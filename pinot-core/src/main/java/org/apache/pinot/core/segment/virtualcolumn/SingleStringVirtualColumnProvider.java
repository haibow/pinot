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
package org.apache.pinot.core.segment.virtualcolumn;

import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.segment.extracolumn.UniqueSingleValueInvertedIndex;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.IntSingleValueDataFileReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.UniqueStringDictionary;


/**
 * Virtual column provider for a virtual column that contains a single string.
 */
public abstract class SingleStringVirtualColumnProvider extends BaseVirtualColumnProvider {

  protected abstract String getValue(VirtualColumnContext context);

  @Override
  public DataFileReader buildReader(VirtualColumnContext context) {
    return new IntSingleValueDataFileReader(0);
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return new UniqueStringDictionary(getValue(context));
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = super.getColumnMetadataBuilder(context);
    columnMetadataBuilder.setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true)
        .setFieldType(FieldSpec.FieldType.DIMENSION).setDataType(FieldSpec.DataType.STRING).setSingleValue(true)
        .setIsSorted(true);
    return columnMetadataBuilder.build();
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(VirtualColumnContext context) {
    return new UniqueSingleValueInvertedIndex(context.getTotalDocCount());
  }
}

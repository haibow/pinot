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

import static org.apache.pinot.common.data.FieldSpec.DataType.*;


/**
 * Factory for extra column providers.
 */
public class ExtraColumnProviderFactory {
  public static ExtraColumnProvider buildProvider(ExtraColumnContext extraColumnContext) {
    FieldSpec fieldSpec = extraColumnContext.getFieldSpec();
    try {
      FieldSpec.DataType dataType = fieldSpec.getDataType().getStoredType();
      if (dataType.equals(STRING)) {
          return (ExtraColumnProvider) Class.forName(UniqueStringExtraColumnProvider.class.getName()).newInstance();
      } else if (dataType.equals(FLOAT)) {
        return (ExtraColumnProvider) Class.forName(UniqueFloatExtraColumnProvider.class.getName()).newInstance();
      } else if (dataType.equals(DOUBLE)) {
        return (ExtraColumnProvider) Class.forName(UniqueDoubleExtraColumnProvider.class.getName()).newInstance();
      } else if (dataType.equals(INT)) {
        return (ExtraColumnProvider) Class.forName(UniqueIntExtraColumnProvider.class.getName()).newInstance();
      } else if (dataType.equals(LONG)) {
        return (ExtraColumnProvider) Class.forName(UniqueLongExtraColumnProvider.class.getName()).newInstance();
      } else if (dataType.equals(BYTES)) {
        return (ExtraColumnProvider) Class.forName(UniqueBytesExtraColumnProvider.class.getName()).newInstance();
      }
      throw new IllegalStateException(
          "Caught exception while creating instance. Unsupported data type: " + dataType.toString());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Caught exception while creating instance", e);
    }
  }
}

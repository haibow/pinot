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
package org.apache.pinot.core.segment.index.readers;

public class UniqueIntDictionary extends BaseImmutableDictionary {
  final Integer _value;

  public UniqueIntDictionary(Integer value) {
    super(1);
    _value = value;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return 0;
  }

  @Override
  public Integer get(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return _value.intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return _value.longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return _value.floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _value.doubleValue();
  }

  @Override
  public String getStringValue(int dictId) {
    return Integer.toString(_value);
  }
}

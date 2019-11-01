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

import java.nio.ByteBuffer;


public class UniqueBytesDictionary extends BaseImmutableDictionary {
  final byte[] _value;

  public UniqueBytesDictionary(byte[] value) {
    super(1);
    _value = value;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return 0;
  }

  @Override
  public byte[] get(int dictId) {
    return _value;
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return ByteBuffer.wrap(_value).getInt();
  }

  @Override
  public long getLongValue(int dictId) {
    return ByteBuffer.wrap(_value).getLong();
  }

  @Override
  public float getFloatValue(int dictId) {
    return ByteBuffer.wrap(_value).getFloat();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return ByteBuffer.wrap(_value).getDouble();
  }

  @Override
  public String getStringValue(int dictId) {
    return ByteBuffer.wrap(_value).toString();
  }
}

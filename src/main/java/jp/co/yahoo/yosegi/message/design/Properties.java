/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.design;

import jp.co.yahoo.yosegi.util.FindClass;

import java.io.IOException;
import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Properties implements Serializable {

  private final Map<String,String> settingContainer;

  public Properties(final Map<String,String> settingContainer) {
    this.settingContainer = settingContainer;
  }

  public Properties() {
    this(new HashMap<String,String>());
  }

  public void set(final String key, final String value) {
    settingContainer.put(key, value);
  }

  /**
   * Convert this object to Map object.
   */
  public Map<String,String> toMap() {
    return settingContainer.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public Set<String> getKey() {
    return settingContainer.keySet();
  }

  public String get(final String key) {
    return get(key, null);
  }

  /**
   * Gets the value of the specified key as String.
   */
  public String get(final String key, final String defaultValue) {
    return settingContainer.getOrDefault(key, defaultValue);
  }

  public int getInt(final String key) {
    return Integer.parseInt(settingContainer.get(key));
  }

  /**
   * Gets the value of the specified key as Int.
   */
  public int getInt(final String key, final int defaultValue) {
    String target = settingContainer.get(key);
    return Objects.isNull(target) ? defaultValue : Integer.parseInt(target);
  }

  public long getLong(final String key) {
    return Long.parseLong(settingContainer.get(key));
  }

  /**
   * Gets the value of the specified key as Long.
   */
  public long getLong(final String key, final long defaultValue) {
    String target = settingContainer.get(key);
    return Objects.isNull(target) ? defaultValue : Long.parseLong(target);
  }

  public double getDouble(final String key) {
    return Double.parseDouble(settingContainer.get(key));
  }

  /**
   * Gets the value of the specified key as Double.
   */
  public double getDouble(final String key, final double defaultValue) {
    String target = settingContainer.get(key);
    return Objects.isNull(target) ? defaultValue : Double.parseDouble(target);
  }

  public boolean containsKey(final String key) throws IOException {
    return settingContainer.containsKey(key);
  }

  /**
   * On the premise that value is a class name, create and acquire a new object.
   */
  public Object getObject(final String key) throws IOException {
    return getObject(key, settingContainer.get(key));
  }

  /**
   * On the premise that value is a class name, create and acquire a new object.
   */
  public Object getObject(final String key, final String defaultValue) throws IOException {
    String targetClassName = settingContainer.get(key);
    if (Objects.isNull(targetClassName) || targetClassName.isEmpty()) {
      targetClassName = defaultValue;
    }
    return FindClass.getObject( targetClassName );
  }

  @Override
  public String toString() {
    return settingContainer.toString();
  }
}


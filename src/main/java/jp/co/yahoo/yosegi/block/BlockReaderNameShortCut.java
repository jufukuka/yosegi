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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.util.Pair;

public final class BlockReaderNameShortCut {

  private static final Pair CLASS_NAME_PAIR = new Pair();

  static {
    CLASS_NAME_PAIR.set( "jp.co.yahoo.yosegi.block.PushdownSupportedBlockReader"   , "B0" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.yosegi.block.EncryptionSupportedBlockReader" , "B1" );
  }

  private BlockReaderNameShortCut() {}

  /**
   * Get the shortcut name from the class name.
   */
  public static String getShortCutName( final String className ) {
    String shortCutName = CLASS_NAME_PAIR.getPair2( className );
    if ( shortCutName == null ) {
      return className;
    }
    return shortCutName;
  }

  /**
   * Get the class name from the shortcut name.
   */
  public static String getClassName( final String shortCutName ) {
    String className = CLASS_NAME_PAIR.getPair1( shortCutName );
    if ( className == null ) {
      return shortCutName;
    }
    return className;
  }

}

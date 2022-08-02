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

package jp.co.yahoo.yosegi.spread.flatten;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryUtil;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.util.List;

public class FlattenColumn {

  private final String linkName;
  private final String[] targetColumnNameArray;

  public FlattenColumn( final String linkName , final String[] targetColumnNameArray ) {
    this.linkName = linkName;
    this.targetColumnNameArray = targetColumnNameArray;
  }

  public String getLinkName() {
    return linkName;
  }

  public String[] getFilterColumnNameArray() {
    return targetColumnNameArray;
  }

  /**
   * Get the target column from ColumnBinaryList.
   */
  public ColumnBinary getColumnBinary( final List<ColumnBinary> columnBinaryList  ) {
    ColumnBinary currentColumnBinary = null;
    List<ColumnBinary> currentColumnBinaryList = columnBinaryList;
    for ( String nodeName : targetColumnNameArray ) {
      if ( currentColumnBinary == null ) {
        currentColumnBinary = ColumnBinaryUtil.getFromColumnName( nodeName , columnBinaryList );
      } else {
        if ( currentColumnBinary.columnType == ColumnType.UNION ) {
          ColumnBinary newCurrent = null;
          List<ColumnBinary> newColumnBinaryList = null;
          for ( ColumnBinary child : currentColumnBinaryList ) {
            if ( child.columnType == ColumnType.SPREAD ) {
              newCurrent = child;
              newColumnBinaryList = newCurrent.columnBinaryList;
              break;
            }
          }
          currentColumnBinary = newCurrent;
          currentColumnBinaryList = newColumnBinaryList;
        }
        if ( currentColumnBinary.columnType != ColumnType.SPREAD ) {
          return null;
        }
        currentColumnBinary =
            ColumnBinaryUtil.getFromColumnName( nodeName , currentColumnBinaryList );
      }
      if ( currentColumnBinary == null ) {
        return null;
      }
      currentColumnBinaryList = currentColumnBinary.columnBinaryList;
    }
    return currentColumnBinary.createRenameColumnBinary( linkName );
  }

  /**
   * Associate the original column name from the reference name of the flattened column.
   */
  public void flattenIndexNode( final BlockIndexNode rootNode ) {
    BlockIndexNode currentNode = rootNode;
    for ( String nodeName : targetColumnNameArray ) {
      currentNode = currentNode.getChildNode( nodeName );
    }
    rootNode.putChildNode( linkName , currentNode );
  }

}

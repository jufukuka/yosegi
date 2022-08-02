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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISpreadLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class DumpSpreadColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Create a new Spread ColumnBinary.
   */
  public static ColumnBinary createSpreadColumnBinary(
      final String columnName ,
      final int columnSize ,
      final List<ColumnBinary> childList ) {
    return new ColumnBinary(
        DumpSpreadColumnBinaryMaker.class.getName() ,
        DefaultCompressor.class.getName() ,
        columnName ,
        ColumnType.SPREAD ,
        columnSize ,
        0 ,
        0 ,
        -1 ,
        new byte[0] ,
        0 ,
        0 ,
        childList );
  }

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    List<IColumn> childColumnList = column.getListColumn();
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();
    for ( IColumn childColumn : childColumnList ) {
      ColumnBinaryMakerCustomConfigNode childNode = null;
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
      if ( currentConfigNode != null ) {
        childNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
        if ( childNode != null ) {
          maker = childNode.getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
        }
      }
      columnBinaryList.add( maker.toBinary(
          commonConfig ,
          childNode ,
          compressResultNode.getChild( childColumn.getColumnName() ) , 
          childColumn ) );
    }

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.SPREAD ,
        column.size() ,
        0 ,
        0 ,
        -1 ,
        new byte[0] ,
        0 ,
        0 ,
        columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return 0;
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    return LoadType.SPREAD;
  }

  @Override
  public void load(
      final ColumnBinary columnBinary , final ILoader loader ) throws IOException {
    if ( loader.getLoaderType() != LoadType.SPREAD ) {
      throw new IOException( "Loader type is not SPREAD." );
    }
    ISpreadLoader spreadLoader = (ISpreadLoader)loader;
    for ( ColumnBinary child : columnBinary.columnBinaryList ) {
      spreadLoader.loadChild( child , loader.getLoadSize() );
    }
    
    spreadLoader.finish();
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      maker.setBlockIndexNode( currentNode , childColumnBinary , spreadIndex );
    }
  }
}

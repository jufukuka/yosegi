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

package jp.co.yahoo.yosegi.stats;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.ByteField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FieldType;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.design.UnionField;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import java.io.IOException;
import java.util.Map;

public final class ColumnStatsToSchemaUtil {

  /**
   * Converts schema into a column statistics.
   */
  public static ColumnStats getColumnStats( final IField schema ) throws IOException {
    if ( ! ( schema instanceof StructContainerField ) ) {
      throw new IOException( "The root of the schema must be a struct." );
    }
    StructContainerField struct = (StructContainerField)schema;
    ColumnStats root = new ColumnStats( "ROOT" );
    for ( String key : struct.getKeys() ) {
      root.addChild( key , convertColumnStatsFromSchema( struct.get( key ) ) );
    }
    root.addSummaryStats( ColumnType.SPREAD , getSummaryStats( struct ) );
    getSchema( root );
    return root;
  }

  /**
   * Converting IField to column stats.
   */
  public static ColumnStats convertColumnStatsFromSchema(
      final IField schema ) throws IOException {
    ColumnStats stats = new ColumnStats( schema.getName() );
    ColumnType columnType = getColumnTypeFromFieldType( schema.getFieldType() );
    switch ( columnType ) {
      case UNION:
        UnionField union = (UnionField)schema;
        for ( String key : union.getKeys() ) {
          IField unionChild = union.get( key );
          stats.addSummaryStats(
              getColumnTypeFromFieldType( unionChild.getFieldType() ) ,
              getSummaryStats( unionChild ) );
        }
        return stats;
      case ARRAY:
        ArrayContainerField array = (ArrayContainerField)schema;
        stats.addChild( "ARRAY" , convertColumnStatsFromSchema( array.getField() ) );
        break;
      case SPREAD:
        StructContainerField struct = (StructContainerField)schema;
        for ( String key : struct.getKeys() ) {
          stats.addChild( key , convertColumnStatsFromSchema( struct.get( key ) ) );
        }
        break;
      default:
        break;
    }
    stats.addSummaryStats( columnType , getSummaryStats( schema ) );
    return stats;
  }
 
  /**
   * Converting IField to summary stats.
   */
  public static SummaryStats getSummaryStats( final IField schema ) throws IOException {
    Properties properties = schema.getProperties();
    long rows = properties.containsKey( "rows" ) ? Long.valueOf( properties.get( "rows" ) ) : 0L;
    long rawDataSize = properties.containsKey( "raw_data_size" )
        ? Long.valueOf( properties.get( "raw_data_size" ) ) : 0L;
    long realDataSize = properties.containsKey( "real_data_size" )
        ? Long.valueOf( properties.get( "real_data_size" ) ) : 0L;
    long logicalDataSize = properties.containsKey( "logical_data_size" )
        ? Long.valueOf( properties.get( "logical_data_size" ) ) : 0L;
    return new SummaryStats( rows , rawDataSize , realDataSize , logicalDataSize , 0 );
  }

  /**
   * Get the Column type from the Field type.
   */
  public static ColumnType getColumnTypeFromFieldType( FieldType type ) {
    switch ( type ) {
      case UNION:
        return ColumnType.UNION;
      case ARRAY:
        return ColumnType.ARRAY;
      case MAP:
      case STRUCT:
        return ColumnType.SPREAD;
      case BOOLEAN:
        return ColumnType.BOOLEAN;
      case BYTE:
        return ColumnType.BYTE;
      case BYTES:
        return ColumnType.BYTES;
      case DOUBLE:
        return ColumnType.DOUBLE;
      case FLOAT:
        return ColumnType.FLOAT;
      case INTEGER:
        return ColumnType.INTEGER;
      case LONG:
        return ColumnType.LONG;
      case SHORT:
        return ColumnType.SHORT;
      case STRING:
        return ColumnType.STRING;
      case NULL:
      default:
        return ColumnType.NULL;
    }
  }

  /**
   * Converts column statistics into a schema.
   */
  public static IField getSchema( final ColumnStats columnStats ) throws IOException {
    Map<ColumnType,SummaryStats> currentColumnMap = columnStats.getSummaryStats();
    if ( currentColumnMap.size() == 0 ) {
      return new NullField( columnStats.getColumnName() );
    }

    // NOTE: If the size is not 1, it is treated as a UNION with multiple types.
    if ( 1 < currentColumnMap.size() ) {
      UnionField union
          = new UnionField( columnStats.getColumnName() );
      int unionChildCount = 0;
      for ( Map.Entry<ColumnType,SummaryStats> entry : currentColumnMap.entrySet() ) {
        ColumnStats unionChild = new ColumnStats( String.format( "_%s" , unionChildCount ) );
        unionChild.addSummaryStats( entry.getKey() , entry.getValue() );
        union.set( getSchema( unionChild ) );
        unionChildCount++;
      }
      return union;
    }

    ColumnType columnType = null;
    SummaryStats summary = null;
    for ( Map.Entry<ColumnType,SummaryStats> entry : currentColumnMap.entrySet() ) {
      columnType = entry.getKey();
      summary = entry.getValue();
    }

    Properties properties = new Properties();
    properties.set( "rows" , Long.toString( summary.getRowCount() ) );
    properties.set( "raw_data_size" , Long.toString( summary.getRawDataSize() ) );
    properties.set( "real_data_size" , Long.toString( summary.getRealDataSize() ) );
    properties.set( "logical_data_size" , Long.toString( summary.getLogicalDataSize() ) );

    switch ( columnType ) {
      case SPREAD:
      case STRUCT:
      case MAP:
        StructContainerField struct
            = new StructContainerField( columnStats.getColumnName() , properties );
        for ( Map.Entry<String,ColumnStats> entry : columnStats.getChildColumnStats().entrySet() ) {
          struct.set( getSchema( entry.getValue() ) );
        }
        return struct;
      case ARRAY:
        IField child = getSchema( columnStats.getChildColumnStats().get( "ARRAY" ) );
        ArrayContainerField array
            = new ArrayContainerField( columnStats.getColumnName() , child , properties );
        return array;
      case BOOLEAN:
        return new BooleanField( columnStats.getColumnName() , properties );
      case BYTE:
        return new ByteField( columnStats.getColumnName() , properties );
      case BYTES:
        return new BytesField( columnStats.getColumnName() , properties );
      case DOUBLE:
        return new DoubleField( columnStats.getColumnName() , properties );
      case FLOAT:
        return new FloatField( columnStats.getColumnName() , properties );
      case INTEGER:
        return new IntegerField( columnStats.getColumnName() , properties );
      case LONG:
        return new LongField( columnStats.getColumnName() , properties );
      case SHORT:
        return new ShortField( columnStats.getColumnName() , properties );
      case STRING:
        return new StringField( columnStats.getColumnName() , properties );

      default:
        return new NullField( columnStats.getColumnName() );
    }
  }

}

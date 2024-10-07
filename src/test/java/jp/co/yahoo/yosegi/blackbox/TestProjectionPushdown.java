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
package jp.co.yahoo.yosegi.blackbox;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.SpreadRawConverter;
import jp.co.yahoo.yosegi.reader.WrapReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.yosegi.spread.expression.*;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestProjectionPushdown {

  @Test
  public void T_pushdown_columnTypeMatch_withPrimitiveType() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestProjectionPushdown.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"primitive\"]]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      assertEquals( spread.getColumnSize() , 1 );
      IColumn column = spread.getColumn( "primitive" );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }

  @Test
  public void T_pushdown_columnTypeMatch_withStructType() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestProjectionPushdown.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"struct\",\"c1\"]]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "struct" ).getColumn( "c1" );
      assertEquals( spread.getColumn( "struct" ).getColumnSize() , 1 );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }

  @Test
  public void T_pushdown_columnTypeMatch_withUnionStructType() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestProjectionPushdown.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"union_struct\",\"c1\"]]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      assertEquals( spread.getColumnSize() , 1 );
      IColumn column = spread.getColumn( "union_struct" ).getColumn( ColumnType.SPREAD ).getColumn( "c1" );
      assertEquals( spread.getColumn( "union_struct" ).getColumn( ColumnType.SPREAD ).getColumnSize() , 1 );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }

  @Test
  public void T_pushdown_columnTypeMatch_withArrayStructType() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestProjectionPushdown.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"array_struct\",\"c1\"]]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "array_struct" ).getColumn(0).getColumn( "c1" );
      assertEquals( spread.getColumn( "array_struct" ).getColumn(0).getColumnSize() , 1 );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }

  @Test
  public void T_pushdown_columnTypeMatch_withUnionArrayStructType() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "blackbox/TestProjectionPushdown.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();

    YosegiReader reader = new YosegiReader();
    WrapReader<Spread> spreadWrapReader = new WrapReader<>(reader, new SpreadRawConverter());
    Configuration readerConfig = new Configuration();
    readerConfig.set( "spread.reader.read.column.names" , "[[\"union_array_struct\",\"c1\"]]" );
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn column = spread.getColumn( "union_array_struct" ).getColumn( ColumnType.ARRAY ).getColumn(0).getColumn( "c1" );
      assertEquals( spread.getColumn( "union_array_struct" ).getColumn( ColumnType.ARRAY ).getColumn(0).getColumnSize() , 1 );
      assertEquals( column.getColumnType() , ColumnType.INTEGER );
    }
  }
}

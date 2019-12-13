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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.*;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

public class TestEncryptionSupportedBlockAboutArraySpreadColumn {

  public static int BLOCK_SIZE = 1024 * 1024 * 2;

  public String getKeyStoreJson() {
    String keyStoreJson = "";
    keyStoreJson += "[";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"name\":\"key_1\",";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"c2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "},";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"name\":\"key_2\",";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"d2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "}";
    keyStoreJson += "]";
    return keyStoreJson;
  }

  public String getReadKeyStoreJson() {
    String keyStoreJson = "";
    keyStoreJson += "[";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"c2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "},";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"d2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "}";
    keyStoreJson += "]";
    return keyStoreJson;
  }

  public String getKeyStoreJsonWithInvalidKey() {
    String keyStoreJson = "";
    keyStoreJson += "[";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"C2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "},";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"D2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "}";
    keyStoreJson += "]";
    return keyStoreJson;
  }

  public String getKeyStoreJsonWhenKey1Only() {
    String keyStoreJson = "";
    keyStoreJson += "[";
    keyStoreJson +=   "{";
    keyStoreJson +=     "\"class\":\"jp.co.yahoo.yosegi.keystore.Base64TextKeyProvider\",";
    keyStoreJson +=     "\"config\":{";
    keyStoreJson +=       "\"text\":\"c2VyY3JldGtleWFlczEyOA==\"";
    keyStoreJson +=     "}";
    keyStoreJson +=   "}";
    keyStoreJson += "]";
    return keyStoreJson;
  }

  public String getColumnEncryptionJson() {
    String json = "";
    json += "{";
    json +=   "\"col4\":{";
    json +=     "\"key_name\":\"key_1\",";
    json +=     "\"child\":{";
    json +=       "\"col4-col2\":{";
    json +=         "\"key_name\":\"key_2\"";
    json +=       "}";
    json +=     "}";
    json +=   "}";
    json += "}";
    return json;
  }

  public String getColumnEncryptionJsonWhenChildOnly() {
    String json = "";
    json += "{";
    json +=   "\"col4\":{";
    json +=     "\"child\":{";
    json +=       "\"col4-col2\":{";
    json +=         "\"key_name\":\"key_2\"";
    json +=       "}";
    json +=     "}";
    json +=   "}";
    json += "}";
    return json;
  }

  public Spread getSpread( final String path ) throws IOException {
    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( path ).openStream() ) );

    Spread spread = new Spread();
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      spread.addParserRow( parser );
      line = in.readLine();
    }

    return spread;
  }

  public Spread getBlock1() throws IOException {
    return getSpread( "block/TestEncryptionSupportedBlock_Spread1.json" );
  }

  public Spread getBlock2() throws IOException {
    return getSpread( "block/TestEncryptionSupportedBlock_Spread2.json" );
  }

  public Spread getBlock3() throws IOException {
    return getSpread( "block/TestEncryptionSupportedBlock_Spread3.json" );
  }

  public Configuration getWriteConfig() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJson() );
    config.set( "column.encrypt.setting" , getColumnEncryptionJson() );
    config.set( "spread.column.maker.default.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    config.set( "block.maker.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    return config;
  }

  public Configuration getWriteConfigWhenChildOnly() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJson() );
    config.set( "column.encrypt.setting" , getColumnEncryptionJsonWhenChildOnly() );
    config.set( "spread.column.maker.default.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    config.set( "block.maker.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    return config;
  }

  public Configuration getWriteConfigWithAadPrefix() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJson() );
    config.set( "column.encrypt.setting" , getColumnEncryptionJson() );
    config.set( "encrypt.aad.prefix" , "this is aad prefix." );
    config.set( "spread.column.maker.default.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    config.set( "block.maker.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    return config;
  }

  public Configuration getWriteConfigWithAadPrefixAndDisableWrite() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJson() );
    config.set( "column.encrypt.setting" , getColumnEncryptionJson() );
    config.set( "encrypt.aad.prefix" , "this is aad prefix." );
    config.set( "encrypt.aad.prefix.disable.write" , "true" );
    config.set( "spread.column.maker.default.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    config.set( "block.maker.compress.class" , "jp.co.yahoo.yosegi.compressor.DefaultCompressor" );
    return config;
  }

  public Configuration getReadConfig() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getReadKeyStoreJson() );
    return config;
  }

  public Configuration getReadConfigWithInvalidKey() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJsonWithInvalidKey() );
    return config;
  }

  public Configuration getReadConfigWhenKey1Only() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJsonWhenKey1Only() );
    return config;
  }

  public Configuration getReadConfigWithNoKey() {
    Configuration config = new Configuration();
    return config;
  }

  public Configuration getReadConfigWithAadPrefix() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getReadKeyStoreJson() );
    config.set( "encrypt.aad.prefix" , "this is aad prefix." );
    return config;
  }

  public Configuration getReadConfigWithAadPrefixAndInvalidKey() {
    Configuration config = new Configuration();
    config.set( "keystore.setting" , getKeyStoreJsonWithInvalidKey() );
    config.set( "encrypt.aad.prefix" , "this is aad prefix." );
    return config;
  }

  public Configuration getReadConfigWithAadPrefixAndNoKey() {
    Configuration config = new Configuration();
    config.set( "encrypt.aad.prefix" , "this is aad prefix." );
    return config;
  }


  @Test
  public void T_encryptColumn_equalsSetValue_withAllBlock() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfig() );
    AdditionalAuthenticationData originalAad = writer.getAad();
    AdditionalAuthenticationDataWithCheckUnique aad;
    if ( originalAad.getPrefix() == null ) {
      aad = new AdditionalAuthenticationDataWithCheckUnique( originalAad.getIdentifier() );
      writer.setAad( aad );
    } else {
      aad = new AdditionalAuthenticationDataWithCheckUnique( originalAad.getPrefix() , originalAad.getIdentifier() );
      writer.setAad( aad );
    }

    Spread s1 = getBlock1();
    Spread s2 = getBlock2();
    Spread s3 = getBlock3();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeFixedBlock( out );
    writer.append( s2.size() , writer.convertRow( s2 ) );
    writer.writeFixedBlock( out );

    List<ColumnBinary> row = writer.convertRow( s3 );
    int sizeAfterAppend = writer.sizeAfterAppend( row );
    writer.append( s3.size() , row );
    int blockSize = writer.size();
    assertEquals( sizeAfterAppend , blockSize );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    assertEquals( blockSize + ( BLOCK_SIZE * 2 ) , blocks.length );

    List<String> aadList = aad.getList();
    Set<String> aadSet = new HashSet<String>();
    for ( String aadString : aadList ) {
      assertFalse( aadSet.contains( aadString ) );
      aadSet.add( aadString );
    }

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfig() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s1.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" ); 
      IColumn col4_1 = col4.getColumn( "col4-col1" ); 

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" ); 
      IColumn col4_2 = col4.getColumn( "col4-col2" ); 

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s2.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s3.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }
  }

  @Test
  public void T_encryptColumn_valueIsNull_withInvalidKey() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfig() );

    Spread s1 = getBlock1();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfigWithInvalidKey() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4Parent = s.getColumn( "col4" );

      assertNull( col4Parent.get(0).getRow() );
      assertNull( col4Parent.get(1).getRow() );
      assertNull( col4Parent.get(2).getRow() );
    }
  }

  @Test
  public void T_encryptColumn_valueIsNull_whenKey1Only() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfig() );

    Spread s1 = getBlock1();
    Spread s2 = getBlock2();
    Spread s3 = getBlock3();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeFixedBlock( out );
    writer.append( s2.size() , writer.convertRow( s2 ) );
    writer.writeFixedBlock( out );

    List<ColumnBinary> row = writer.convertRow( s3 );
    int sizeAfterAppend = writer.sizeAfterAppend( row );
    writer.append( s3.size() , row );
    int blockSize = writer.size();
    assertEquals( sizeAfterAppend , blockSize );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfigWhenKey1Only() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s1.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertNull( col4_2.get(i).getRow() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s2.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );
                                                                                                    assertNull( col4_2.get(i).getRow() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s3.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );
                                                                                                    assertNull( col4_2.get(i).getRow() );
      }
    }
  }

  @Test
  public void T_encryptColumn_valueIsNull_withNoKey() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfig() );

    Spread s1 = getBlock1();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfigWithNoKey() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );
    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4Parent = s.getColumn( "col4" );

      assertNull( col4Parent.get(0).getRow() );
      assertNull( col4Parent.get(1).getRow() );
      assertNull( col4Parent.get(2).getRow() );
    }
  }

  @Test
  public void T_encryptChildColumn_equalsSetValue_withAllBlock() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfigWhenChildOnly() );
    AdditionalAuthenticationData originalAad = writer.getAad();
    AdditionalAuthenticationDataWithCheckUnique aad;
    if ( originalAad.getPrefix() == null ) {
      aad = new AdditionalAuthenticationDataWithCheckUnique( originalAad.getIdentifier() );
      writer.setAad( aad );
    } else {
      aad = new AdditionalAuthenticationDataWithCheckUnique( originalAad.getPrefix() , originalAad.getIdentifier() );
      writer.setAad( aad );
    }

    Spread s1 = getBlock1();
    Spread s2 = getBlock2();
    Spread s3 = getBlock3();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeFixedBlock( out );
    writer.append( s2.size() , writer.convertRow( s2 ) );
    writer.writeFixedBlock( out );

    List<ColumnBinary> row = writer.convertRow( s3 );
    int sizeAfterAppend = writer.sizeAfterAppend( row );
    writer.append( s3.size() , row );
    int blockSize = writer.size();
    assertEquals( sizeAfterAppend , blockSize );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    assertEquals( blockSize + ( BLOCK_SIZE * 2 ) , blocks.length );

    List<String> aadList = aad.getList();
    Set<String> aadSet = new HashSet<String>();
    for ( String aadString : aadList ) {
      assertFalse( aadSet.contains( aadString ) );
      aadSet.add( aadString );
    }

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfig() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s1.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s2.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s3.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertEquals( ( (PrimitiveObject)( col4_2.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_2Original.get(i).getRow() ) ).getString() );
      }
    }
  }

  @Test
  public void T_encryptChildColumn_valueIsNull_withInvalidKey() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfigWhenChildOnly() );

    Spread s1 = getBlock1();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfigWithInvalidKey() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s1.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertNull( col4_2.get(i).getRow() );
      }
    }
  }

  @Test
  public void T_encryptChildColumn_valueIsNull_withNoKey() throws IOException {
    EncryptionSupportedBlockWriter writer = new EncryptionSupportedBlockWriter();
    writer.setup( BLOCK_SIZE , getWriteConfigWhenChildOnly() );

    Spread s1 = getBlock1();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    writer.append( s1.size() , writer.convertRow( s1 ) );
    writer.writeVariableBlock( out );
    byte[] blocks = out.toByteArray();
    writer.close();

    EncryptionSupportedBlockReader reader = new EncryptionSupportedBlockReader();
    reader.setup( getReadConfigWithNoKey() );
    ByteArrayInputStream in = new ByteArrayInputStream( blocks );

    reader.setStream( in , BLOCK_SIZE );

    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn col4ParentOriginal = s1.getColumn( "col4" );
      IColumn col4Original = col4ParentOriginal.getColumn( 0 );
      IColumn col4Parent = s.getColumn( "col4" );
      IColumn col4 = col4Parent.getColumn( 0 );

      IColumn col4_1Original = col4Original.getColumn( "col4-col1" );
      IColumn col4_1 = col4.getColumn( "col4-col1" );

      IColumn col4_2Original = col4Original.getColumn( "col4-col2" );
      IColumn col4_2 = col4.getColumn( "col4-col2" );

      for ( int i = 0 ; i < col4Parent.size() ; i++ ) {
        assertEquals( ( (PrimitiveObject)( col4_1.get(i).getRow() ) ).getString() ,
          ( (PrimitiveObject)( col4_1Original.get(i).getRow() ) ).getString() );

        assertNull( col4_2.get(i).getRow() );
      }
    }
  }

}

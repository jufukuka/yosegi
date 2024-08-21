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

package jp.co.yahoo.yosegi.message.design.openlinege;

import com.fasterxml.jackson.databind.SerializationFeature;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.ByteField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
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
import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OpenLinegeJsonUtil {

  /**
   * Creates yosegi schema from openlinege json.
   */
  public static IField jsonToSchema( final InputStream in ) throws IOException {
    return parserToSchema( new JacksonMessageReader().create( in ) );
  }

  /**
   * Creates yosegi schema from openlinege json.
   */
  public static IField jsonToSchema( final byte[] json ) throws IOException {
    return parserToSchema( new JacksonMessageReader().create( json ) );
  }

  /**
   * Creates yosegi schema from openlinege parser.
   */
  public static IField parserToSchema( final IParser parser ) throws IOException {
    if ( parser.size() == 0 ) {
      throw new IOException( "Root must be of type Struct." );
    }
    StructContainerField schema = new StructContainerField( "ROOT" );
    IParser childs = parser.getParser( "fields" );
    for ( int i = 0; i < childs.size(); i++ ) {
      schema.set( parserToField( childs.getParser(i) ) );
    }
    return schema;
  }

  /**
   * Creates yosegi schema from openlinege field.
   */
  public static IField parserToField( final IParser field ) throws IOException {
    if ( ! field.containsKey( "name" ) || ! field.containsKey( "type" ) ) {
      throw new IOException( "name or type is empty." );
    }
    String name = field.get("name").getString();
    String type = field.get("type").getString();
    IParser propertiesParser = field.getParser( "properties" );
    String[] keys = propertiesParser.getAllKey();
    Properties properties = new Properties();
    for ( int i = 0; i < propertiesParser.size(); i++ ) {
      properties.set( keys[i] , propertiesParser.get(keys[i]).getString() );
    }
    switch ( type ) {
      case "union":
        UnionField union = new UnionField( name , properties );
        IParser unionChildFields = field.getParser( "fields" );
        for ( int i = 0; i < unionChildFields.size(); i++ ) {
          union.set( parserToField( unionChildFields.getParser(i) ) );
        }
        return union;
      case "array":
        IParser arrayChildFields = field.getParser( "fields" );
        return new ArrayContainerField(
            name , parserToField( arrayChildFields.getParser(0) ) , properties );
      case "map":
      case "struct":
        StructContainerField struct = new StructContainerField( name , properties );
        IParser childFields = field.getParser( "fields" );
        for ( int i = 0; i < childFields.size(); i++ ) {
          struct.set( parserToField( childFields.getParser(i) ) );
        }
        return struct;
      case "boolean":
        return new BooleanField( name , properties );
      case "byte":
        return new ByteField( name , properties );
      case "bytes":
        return new BytesField( name , properties );
      case "double":
        return new DoubleField( name , properties );
      case "float":
        return new FloatField( name , properties );
      case "int":
        return new IntegerField( name , properties );
      case "long":
        return new LongField( name , properties );
      case "short":
        return new ShortField( name , properties );
      case "string":
        return new StringField( name , properties );
      case "null":
        return new NullField( name , properties );
      default:
        throw new IOException( "Unsupported type: " + type );
    }
  }

  /**
   * Creates open lineage JSON from a schema.
   */
  public static byte[] schemaToJson( final IField schema ) throws IOException {
    if ( ! ( schema instanceof StructContainerField ) ) {
      throw new IOException( "Root must be of type Struct." );
    }
    StructContainerField structSchema = (StructContainerField)schema;
    Map<Object, Object> jsonNodeMap = new LinkedHashMap<Object, Object>();
    List<Map<String,Object>> field = new ArrayList<Map<String,Object>>();
    jsonNodeMap.put( "fields" , field );
    for ( String key : structSchema.getKeys() ) {
      field.add( getField( structSchema.get( key ) ) );
    }
    return new JacksonMessageWriter( SerializationFeature.INDENT_OUTPUT ).create( jsonNodeMap );
  }

  /**
   * Create a field with open lineage.
   */
  public static Map<String,Object> getField( final IField schema ) throws IOException {
    Map<String,Object> fields = new LinkedHashMap<String,Object>();
    Map<String,Object> properties = new LinkedHashMap<String,Object>();
    fields.put( "name" , schema.getName() );
    fields.put( "type" , getType( schema ) );
    for ( Map.Entry<String,String> entry : schema.getProperties().toMap().entrySet() ) {
      properties.put( entry.getKey() , entry.getValue() );
    }
    fields.put( "properties" , properties );
    switch ( schema.getFieldType() ) {
      case UNION:
        List<Map<String,Object>> unionChildFields = new ArrayList<Map<String,Object>>();
        UnionField union = (UnionField)schema;
        String[] unionKeys = union.getKeys();
        for ( int i = 0; i < unionKeys.length; i++ ) {
          Map<String,Object> unionChildField = getField( union.get( unionKeys[i] ) );
          unionChildField.put( "name" , String.format( "_%s" , i ) );
          unionChildFields.add( unionChildField );
        }
        fields.put( "fields" , unionChildFields );
        break;
      case STRUCT:
        List<Map<String,Object>> childFields = new ArrayList<Map<String,Object>>();
        StructContainerField struct = (StructContainerField)schema;
        for ( String key : struct.getKeys() ) {
          childFields.add( getField( struct.get( key ) ) );
        }
        fields.put( "fields" , childFields );
        break;
      case ARRAY:
        List<Map<String,Object>> arrayFields = new ArrayList<Map<String,Object>>();
        ArrayContainerField array = (ArrayContainerField)schema;
        Map<String,Object> arrayChildField = getField( array.getField() );
        arrayChildField.put( "name" , "_element" );
        arrayFields.add( arrayChildField );
        fields.put( "fields" , arrayFields );
        break;
      default:
        break;
    }
    return fields;
  }

  /**
   * Converts a schema type to an open lineage type.
   */
  public static String getType( final IField schema ) throws IOException {
    switch ( schema.getFieldType() ) {
      case UNION:
        return "union";
      case ARRAY:
        return "array";
      case MAP:
        return "map";
      case STRUCT:
        return "struct";
      case BOOLEAN:
        return "boolean";
      case BYTE:
        return "byte";
      case BYTES:
        return "bytes";
      case DOUBLE:
        return "double";
      case FLOAT:
        return "float";
      case INTEGER:
        return "int";
      case LONG:
        return "long";
      case SHORT:
        return "short";
      case STRING:
        return "string";
      case NULL:
        return "null";
      default:
        throw new IOException( "Unsupported type: " + schema.getFieldType() );
    }
  }

}

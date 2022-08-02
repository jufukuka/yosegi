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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestConstantColumnBinaryMaker {

  public IColumn toColumn(
      final ColumnBinary columnBinary, final int[] repetitions, final int loadSize)
      throws IOException {
    if (repetitions != null) {
      columnBinary.setRepetitions(repetitions, loadSize);
    }
    return toColumn(columnBinary);
  }

  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount = columnBinary.isSetLoadSize ? columnBinary.loadSize : columnBinary.rowCount;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  public int getLoadSize(final int[] repetitions) {
    if (repetitions == null) {
      return 0;
    }
    int loadSize = 0;
    for (int size : repetitions) {
      loadSize += size;
    }
    return loadSize;
  }

  @Test
  public void T_createBoolean_equals_whenLoadSizeEqualsRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBoolean_equals_whenLoadSizeLessThanRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBoolean_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBoolean_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createBoolean_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBoolean_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BooleanObj( true ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BOOLEAN );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( true , ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean() );
    assertEquals( true , ( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLoadSizeEqualsRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLoadSizeLessThanRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createByte_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    byte value = (byte)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ByteObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTE );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLoadSizeEqualsRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLoadSizeLessThanRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getShort() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createShort_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    short value = (short)100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getShort() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getShort() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createBinary_short_1() throws IOException{
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( (short)20 ) , "hoge" , 3 );

    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    IColumn decodeColumn = toColumn(columnBinary);
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getShort() );
    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getShort() );
    assertEquals( (short)20 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getShort() );
    assertNull( decodeColumn.get(3).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLoadSizeEqualsRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLoadSizeLessThanRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getInt() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createInt_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    int value = 100;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new IntegerObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getInt() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getInt() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createLong_equals_whenLoadSizeEqualsRowCount() throws IOException{
    long value = 100L;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createLong_equals_whenLoadSizeLessThanRowCount() throws IOException{
    long value = 100L;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createLong_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    long value = 100L;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createLong_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    long value = 100L;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createLong_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    long value = 100L;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new LongObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.LONG );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getLong() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() );
    //assertEquals( value , ( (PrimitiveObject)( column.get(5).getRow() ) ).getLong() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLoadSizeEqualsRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getFloat() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLoadSizeLessThanRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getFloat() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getFloat() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getFloat() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createFloat_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    float value = 100f;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new FloatObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.FLOAT );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getFloat() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getFloat() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLoadSizeEqualsRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLoadSizeLessThanRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getDouble() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createDouble_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    double value = 100d;
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getDouble() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getDouble() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createString_equals_whenLoadSizeEqualsRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createString_equals_whenLoadSizeLessThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createString_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createString_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createString_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createString_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new StringObj( value ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLoadSizeEqualsRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , columnBinary.rowCount );

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLoadSizeLessThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 2 );

    assertEquals( column.size() , 2 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLoadSizeGreaterThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn column = factory.create( columnBinary , 4 );

    assertEquals( column.size() , 4 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLastLoadIndexEqualsRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    int[] repetitions = new int[] {2, 1, 2};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 5 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() );
    assertNull( column.get(5).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLastLoadIndexLessThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );
    int[] repetitions = new int[] {2, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 3 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertNull( column.get(3).getRow() );
  }

  @Test
  public void T_createBytes_equals_whenLastLoadIndexGeraterThanRowCount() throws IOException{
    String value = "100";
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( new BytesObj( value.getBytes() ) , "hoge" , 3 );
    assertEquals( columnBinary.columnName , "hoge" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.BYTES );

    int[] repetitions = new int[] {2, 1, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = toColumn(columnBinary, repetitions, loadSize);

    assertEquals( column.size() , 6 );

    assertEquals( value , ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() );
    assertEquals( value , ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
  }

}

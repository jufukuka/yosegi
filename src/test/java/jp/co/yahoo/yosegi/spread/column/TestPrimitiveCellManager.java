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
package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.spread.column.filter.NotNullFilter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestPrimitiveCellManager {

  @Test
  public void T_newInstance_void_withStringCellMaker() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
  }

  @Test
  public void T_newInstance_throwsException_withNull() throws IOException {
    assertThrows( IOException.class ,
      () -> {
        PrimitiveCellManager cellManager = new PrimitiveCellManager( null );
      }
    );
  }

  @Test
  public void T_addAndGet_resultIsCreatedStringObj() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 0 );
    ICell cell = cellManager.get( 0 , null );
    assertTrue( ( cell instanceof StringCell ) );
    PrimitiveObject obj = (PrimitiveObject)( cell.getRow() );
    assertEquals( obj.getString() , "test" );
  }

  @Test
  public void T_addAndGet_resultIsCreatedSameStringObjs() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 0 );
    cellManager.add( new StringObj( "test1" ) , 1 );

    ICell cell0 = cellManager.get( 0 , null );
    assertEquals( ( (PrimitiveObject)( cell0.getRow() ) ).getString() , "test" );

    ICell cell1 = cellManager.get( 1 , null );
    assertEquals( ( (PrimitiveObject)( cell1.getRow() ) ).getString() , "test1" );
  }

  @Test
  public void T_addAndGet_skippedCellIsNull() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 3 );

    assertNull( cellManager.get( 0 , null ) );
    assertNull( cellManager.get( 1 , null ) );
    assertNull( cellManager.get( 2 , null ) );

    ICell cell0 = cellManager.get( 3 , null );
    assertEquals( ( (PrimitiveObject)( cell0.getRow() ) ).getString() , "test" );
  }

  @Test
  public void T_add_throwsException_whenIndexIsAlreadySet() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 3 );

    assertThrows( RuntimeException.class ,
      () -> {
        cellManager.add( new StringObj( "test" ) , 3 );
      }
    );
  }

  @Test
  public void T_add_throwsException_whenLessThanCurrentIndex() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 3 );

    assertThrows( RuntimeException.class ,
      () -> {
        cellManager.add( new StringObj( "test" ) , 1 );
      }
    );
  }

  @Test
  public void T_add_throwsException_whenBelowZero() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );

    assertThrows( RuntimeException.class ,
      () -> {
        cellManager.add( new StringObj( "test" ) , -1 );
      }
    );
  }

  @Test
  public void T_addAndGet_skippedCellIsNull_whenSetDistantIndex() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 0 );
    cellManager.add( new StringObj( "test1" ) , 5 );

    ICell cell0 = cellManager.get( 0 , null );
    assertEquals( ( (PrimitiveObject)( cell0.getRow() ) ).getString() , "test" );

    assertNull( cellManager.get( 1 , null ) );
    assertNull( cellManager.get( 2 , null ) );
    assertNull( cellManager.get( 3 , null ) );
    assertNull( cellManager.get( 4 , null ) );
    ICell cell1 = cellManager.get( 5 , null );
    assertEquals( ( (PrimitiveObject)( cell1.getRow() ) ).getString() , "test1" );
  }

  @Test
  public void T_addAndGet_skippedCellIsNull_whenStartIndexIsGreaterThanZeroAndSetDistantIndex() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 3 );
    cellManager.add( new StringObj( "test1" ) , 5 );

    assertNull( cellManager.get( 0 , null ) );
    assertNull( cellManager.get( 1 , null ) );
    assertNull( cellManager.get( 2 , null ) );

    ICell cell0 = cellManager.get( 3 , null );
    assertEquals( ( (PrimitiveObject)( cell0.getRow() ) ).getString() , "test" );

    assertNull( cellManager.get( 4 , null ) );
    ICell cell1 = cellManager.get( 5 , null );
    assertEquals( ( (PrimitiveObject)( cell1.getRow() ) ).getString() , "test1" );
  }

  @Test
  public void T_size_resultIsNumberOfAdd() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 0 );

    assertEquals( cellManager.size() , 1 );
  }

  @Test
  public void T_size_resultIsNumberOfAddAndNumberOfNulls() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 5 );

    assertEquals( cellManager.size() , 6 );
  }

  @Test
  public void T_clear_void() throws IOException {
    ICellMaker maker = CellMakerFactory.getCellMaker( ColumnType.STRING );
    PrimitiveCellManager cellManager = new PrimitiveCellManager( maker );
    cellManager.add( new StringObj( "test" ) , 5 );
    assertEquals( cellManager.size() , 6 );

    cellManager.clear();
    assertEquals( cellManager.size() , 0 );
  }

}

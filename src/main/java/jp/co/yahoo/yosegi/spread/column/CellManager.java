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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.util.IndexAndObject;
import jp.co.yahoo.yosegi.util.RangeBinarySearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CellManager implements ICellManager<ICell> {

  private final RangeBinarySearch<ICell> rangeBinarySearch =
      new RangeBinarySearch<ICell>();

  @Override
  public void add( final ICell cell , final int index ) {
    rangeBinarySearch.add( cell , index );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    ICell cell = rangeBinarySearch.get( index );
    if ( cell == null ) {
      return defaultCell;
    }
    return cell;
  }

  @Override
  public int size() {
    return rangeBinarySearch.size();
  }

  @Override
  public void clear() {
    rangeBinarySearch.clear();
  }

}

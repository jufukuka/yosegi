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
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionary;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.PrimitiveObjectDictionary;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ShortColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.DetermineMinMax;
import jp.co.yahoo.yosegi.util.DetermineMinMaxFactory;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.diffencoder.INumEncoder;
import jp.co.yahoo.yosegi.util.io.diffencoder.NumEncoderUtil;
import jp.co.yahoo.yosegi.util.io.nullencoder.NullBinaryEncoder;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizedNullArrayLongColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Create a numeric column from the entered type.
   */
  public static PrimitiveObject createConstObjectFromNum(
      final ColumnType type , final long num ) throws IOException {
    switch ( type ) {
      case BYTE:
        return new ByteObj( Long.valueOf( num ).byteValue() );
      case SHORT:
        return new ShortObj( Long.valueOf( num ).shortValue() );
      case INTEGER:
        return new IntegerObj( Long.valueOf( num ).intValue() );
      default:
        return new LongObj( num );
    }
  }

  // Metadata layout
  // byteOrder, ColumnStart, rowCount , dicSize , nullIndexLength, indexLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 5;

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
    Map<Long,Integer> dicMap = new HashMap<Long,Integer>();
    long[] dicArray = new long[column.size()];
    int[] indexArray = new int[column.size()];
    boolean[] isNullArray = new boolean[column.size()];

    DetermineMinMax<Long> detemineMinMax = DetermineMinMaxFactory.createLong();
    int rowCount = 0;
    int nullCount = 0;
    int nullMaxIndex = 0;
    int notNullMaxIndex = 0;

    int startIndex = 0;
    for ( ; startIndex < column.size() ; startIndex++ ) {
      ICell cell = column.get(startIndex);
      if ( cell.getType() != ColumnType.NULL ) {
        break;
      }
    }

    for ( int i = startIndex,arrayIndex = 0 ; i < column.size() ; i++ ,arrayIndex++) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = arrayIndex;
        isNullArray[arrayIndex] = true;
        continue;
      }
      notNullMaxIndex = arrayIndex;
      PrimitiveCell primitiveCell = (PrimitiveCell) cell;
      PrimitiveObject primitiveObj = primitiveCell.getRow();
      Long target = Long.valueOf( primitiveObj.getLong() );

      if ( ! dicMap.containsKey( target ) ) {
        detemineMinMax.set( target );
        int dicIndex = dicMap.size();
        dicMap.put( target , dicIndex );
        dicArray[dicIndex] = target.longValue();
      }
      indexArray[rowCount] = dicMap.get( target );
      rowCount++;
    }

    if ( nullCount == 0
        && detemineMinMax.getMin().equals( detemineMinMax.getMax() )
        && startIndex == 0 ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          createConstObjectFromNum( column.getColumnType() , detemineMinMax.getMin() ) ,
          column.getColumnName() ,
          column.size() );
    }

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicMap.size() );

    int indexLength = indexConverter.calcBinarySize( rowCount );

    INumEncoder dicEncoder =
        NumEncoderUtil.createEncoder( detemineMinMax.getMin() , detemineMinMax.getMax() );
    int dicLength = dicEncoder.calcBinarySize( dicMap.size() );

    ByteOrder order = ByteOrder.nativeOrder();

    int nullIndexLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    byte[] binaryRaw = new byte[ META_LENGTH + nullIndexLength + indexLength + dicLength ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.put( order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1 );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.putInt( dicMap.size() );
    wrapBuffer.putInt( nullIndexLength );
    wrapBuffer.putInt( indexLength );
    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullIndexLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );
    IWriteSupporter indexWriter = indexConverter.toWriteSuppoter(
        rowCount , binaryRaw , META_LENGTH + nullIndexLength , indexLength  );
    for ( int i = 0 ; i < rowCount ; i++ ) {
      indexWriter.putInt( indexArray[i] );
    }
    dicEncoder.toBinary(
        dicArray ,
        binaryRaw ,
        META_LENGTH + nullIndexLength + indexLength,
        dicMap.size() ,
        order );

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Long.BYTES * 2 + compressBinary.length ];

    wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( detemineMinMax.getMin() );
    wrapBuffer.putLong( detemineMinMax.getMax() );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binaryRaw.length ,
        NumEncoderUtil.getLogicalSize( rowCount , column.getColumnType() ) ,
        dicMap.size() ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    int startIndex = analizeResult.getRowStart();
    int maxIndex = analizeResult.getRowEnd();
    int nullCount = analizeResult.getNullCount() - startIndex;
    int notNullCount = analizeResult.getRowCount();

    int nullIndexLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );
    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , analizeResult.getUniqCount() );

    int indexLength = indexConverter.calcBinarySize( notNullCount );

    long min;
    long max;
    switch ( analizeResult.getColumnType() ) {
      case BYTE:
        min = (long)( (ByteColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ByteColumnAnalizeResult) analizeResult ).getMax();
        break;
      case SHORT:
        min = (long)( (ShortColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ShortColumnAnalizeResult) analizeResult ).getMax();
        break;
      case INTEGER:
        min = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMax();
        break;
      case LONG:
        min = ( (LongColumnAnalizeResult) analizeResult ).getMin();
        max = ( (LongColumnAnalizeResult) analizeResult ).getMax();
        break;
      default:
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
        break;
    }
    int dicLength;
    try {
      INumEncoder valueEncoder =
          NumEncoderUtil.createEncoder( min , max );
      dicLength = valueEncoder.calcBinarySize( analizeResult.getUniqCount() );
    } catch ( IOException ex ) {
      throw new UncheckedIOException( ex );
    }

    return META_LENGTH + nullIndexLength + indexLength + dicLength;
  }

  @Override
  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount =
        (columnBinary.loadIndex == null) ? columnBinary.rowCount : columnBinary.loadIndex.length;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    if (columnBinary.loadIndex == null) {
      return LoadType.SEQUENTIAL;
    } else {
      return LoadType.DICTIONARY;
    }
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ByteBuffer compressWrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    long min = compressWrapBuffer.getLong();
    long max = compressWrapBuffer.getLong();
    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);

    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowCount = wrapBuffer.getInt();
    int dicSize = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int indexLength = wrapBuffer.getInt();

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter(0, dicSize);

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullIndexLength);

    INumEncoder dicEncoder = NumEncoderUtil.createEncoder(min, max);
    IDictionary dic = new PrimitiveObjectDictionary(dicSize);
    dicEncoder.setDictionary(
        binary, META_LENGTH + nullIndexLength + indexLength, dicSize, order, dic);

    IReadSupporter indexReader =
        indexConverter.toReadSupporter(binary, META_LENGTH + nullIndexLength, indexLength);
    int index = startIndex;
    for (; index < startIndex; index++) {
      loader.setNull(index);
    }
    for (int i = 0; i < isNullArray.length; i++, index++) {
      if (isNullArray[i]) {
        loader.setNull(index);
      } else {
        loader.setLong(index, dic.getPrimitiveObject(indexReader.getInt()).getLong());
      }
    }
    // NOTE: null padding up to load size
    for (int i = index; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    ByteBuffer compressWrapBuffer =
        ByteBuffer.wrap(columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    long min = compressWrapBuffer.getLong();
    long max = compressWrapBuffer.getLong();
    int start = columnBinary.binaryStart + (Long.BYTES * 2);
    int length = columnBinary.binaryLength - (Long.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);

    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowCount = wrapBuffer.getInt();
    int dicSize = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int indexLength = wrapBuffer.getInt();

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter(0, dicSize);

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullIndexLength);

    INumEncoder dicEncoder = NumEncoderUtil.createEncoder(min, max);
    IDictionary dic = new PrimitiveObjectDictionary(dicSize);
    dicEncoder.setDictionary(
        binary, META_LENGTH + nullIndexLength + indexLength, dicSize, order, dic);

    IReadSupporter indexReader =
        indexConverter.toReadSupporter(binary, META_LENGTH + nullIndexLength, indexLength);

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int previousLoadIndex = -1;
    int lastIndex = startIndex + isNullArray.length - 1;
    for (int loadIndex : columnBinary.loadIndex) {
      if (loadIndex < 0) {
        throw new IOException("Index must be equal to or greater than 0.");
      }
      if (loadIndex < previousLoadIndex) {
        throw new IOException("Index must be equal to or greater than the previous number.");
      }
      if (loadIndex > lastIndex) {
        break;
      }
      if (loadIndex >= startIndex && !isNullArray[loadIndex - startIndex]) {
        if (previousLoadIndex != loadIndex) {
          dictionarySize++;
        }
      }
      previousLoadIndex = loadIndex;
    }
    loader.createDictionary(dictionarySize);

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    //   Set dictionaryIndex: loadIndexArrayOffset, dictionaryIndex
    previousLoadIndex = -1; // NOTE: reset
    int loadIndexArrayOffset = 0;
    int dictionaryIndex = -1;
    int readOffset = startIndex;
    long value = 0;
    for (int loadIndex : columnBinary.loadIndex) {
      if (loadIndex > lastIndex) {
        break;
      }
      // NOTE: read skip
      for (; readOffset <= loadIndex; readOffset++) {
        if (readOffset >= startIndex && !isNullArray[readOffset - startIndex]) {
          value = dic.getPrimitiveObject(indexReader.getInt()).getLong();
        }
      }
      if (loadIndex < startIndex || isNullArray[loadIndex - startIndex]) {
        loader.setNull(loadIndexArrayOffset);
      } else {
        if (previousLoadIndex != loadIndex) {
          dictionaryIndex++;
          loader.setLongToDic(dictionaryIndex, value);
        }
        loader.setDictionaryIndex(loadIndexArrayOffset, dictionaryIndex);
      }
      previousLoadIndex = loadIndex;
      loadIndexArrayOffset++;
    }

    // NOTE: null padding up to load size
    for (int i = loadIndexArrayOffset; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  @Override
  public void load(final ColumnBinary columnBinary, final ILoader loader) throws IOException {
    if (columnBinary.loadIndex == null) {
      if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
        throw new IOException("Loader type is not SEQUENTIAL.");
      }
      loadFromColumnBinary(columnBinary, (ISequentialLoader) loader);
    } else {
      if (loader.getLoaderType() != LoadType.DICTIONARY) {
        throw new IOException("Loader type is not DICTIONARY.");
      }
      loadFromExpandColumnBinary(columnBinary, (IDictionaryLoader) loader);
    }
    loader.finish();
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer compressWrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    long min = compressWrapBuffer.getLong();
    long max = compressWrapBuffer.getLong();
    int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
    int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowCount = wrapBuffer.getInt();
    int dicSize = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int indexLength = wrapBuffer.getInt();

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicSize );

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength );

    INumEncoder dicEncoder =
        NumEncoderUtil.createEncoder( min , max );
    IDictionary dic = allocator.createDictionary( dicSize );
    dicEncoder.setDictionary(
        binary ,
        META_LENGTH + nullIndexLength + indexLength,
        dicSize,
        order,
        dic );

    allocator.setValueCount( startIndex + isNullArray.length );

    IReadSupporter indexReader =
        indexConverter.toReadSupporter( binary , META_LENGTH + nullIndexLength , indexLength );
    int index = startIndex;
    for ( ; index < startIndex ; index++ ) {
      allocator.setNull( index );
    }
    for ( int i = 0 ; i < isNullArray.length ; i++,index++ ) {
      if ( isNullArray[i]  ) {
        allocator.setNull( index );
      } else {
        allocator.setFromDictionary( index , indexReader.getInt() , dic );
      }
    }
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

  public class ColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private final long min;
    private final long max;

    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Init.
     */
    public ColumnManager( final long min , final long max , final ColumnBinary columnBinary ) {
      this.min = min;
      this.max = max;
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
      int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

      ByteOrder order = wrapBuffer.get() == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final int startIndex = wrapBuffer.getInt();
      int rowCount = wrapBuffer.getInt();
      int dicSize = wrapBuffer.getInt();
      int nullIndexLength = wrapBuffer.getInt();
      int indexLength = wrapBuffer.getInt();
      int dicLength = binary.length - META_LENGTH - nullIndexLength - indexLength;

      NumberToBinaryUtils.IIntConverter indexConverter =
          NumberToBinaryUtils.getIntConverter( 0 , dicSize );

      boolean[] isNullArray =
          NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength ); 

      IReadSupporter indexReader =
          indexConverter.toReadSupporter( binary , META_LENGTH + nullIndexLength , indexLength );
      int[] indexArray = new int[isNullArray.length];
      for ( int i = 0 ; i < indexArray.length ; i++ ) {
        if ( ! isNullArray[i] ) {
          indexArray[i] = indexReader.getInt();
        }
      }

      INumEncoder dicEncoder =
          NumEncoderUtil.createEncoder( min , max );
      PrimitiveObject[] dicArray = dicEncoder.toPrimitiveArray(
          binary ,
          META_LENGTH + nullIndexLength + indexLength,
          dicSize,
          order );

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new OptimizedNullArrayDicCellManager(
          columnBinary.columnType , startIndex , isNullArray , indexArray , dicArray ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      if ( ! isCreate ) {
        try {
          create();
        } catch ( IOException ex ) {
          throw new UncheckedIOException( ex );
        }
      }
      return column;
    }

    @Override
    public List<String> getColumnKeys() {
      return new ArrayList<String>();
    }

    @Override
    public int getColumnSize() {
      return 0;
    }

  }

}

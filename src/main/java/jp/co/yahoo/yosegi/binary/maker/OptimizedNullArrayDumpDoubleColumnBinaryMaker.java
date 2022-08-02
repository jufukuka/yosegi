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
import jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.DoubleColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.DetermineMinMax;
import jp.co.yahoo.yosegi.util.DetermineMinMaxFactory;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
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

public class OptimizedNullArrayDumpDoubleColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, nullIndexLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 2;

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
    double[] doubleArray = new double[column.size()];
    boolean[] isNullArray = new boolean[column.size()];

    DetermineMinMax<Double> detemineMinMax = DetermineMinMaxFactory.createDouble();
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

    for ( int i = startIndex,arrayIndex = 0 ; i < column.size() ; i++,arrayIndex++ ) {
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
      Double target = Double.valueOf( primitiveObj.getDouble() );
      detemineMinMax.set( target );
      doubleArray[rowCount] = target.doubleValue();
      rowCount++;
    }

    if ( nullCount == 0
        && detemineMinMax.getMin().equals( detemineMinMax.getMax() )
        && startIndex == 0 ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new DoubleObj( detemineMinMax.getMin() ) , column.getColumnName() , column.size() );
    }

    ByteOrder order = ByteOrder.nativeOrder();
    int nullIndexLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );
    int valueLength = Double.BYTES * rowCount;
    byte[] binaryRaw = new byte[ META_LENGTH + nullIndexLength + valueLength ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.put( order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1 );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( nullIndexLength );
    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullIndexLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );
    IWriteSupporter valueWriter = ByteBufferSupporterFactory.createWriteSupporter(
        binaryRaw ,
        META_LENGTH + nullIndexLength ,
        valueLength,
        order );
    for ( int i = 0 ; i < rowCount ; i++ ) {
      valueWriter.putDouble( doubleArray[i] );
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Double.BYTES * 2 + compressBinary.length ];

    wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putDouble( detemineMinMax.getMin() );
    wrapBuffer.putDouble( detemineMinMax.getMax() );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binaryRaw.length ,
        Double.BYTES * rowCount ,
        -1 ,
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

    int valueLength = Double.BYTES * notNullCount;

    return META_LENGTH + nullIndexLength + valueLength;
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    if (columnBinary.isSetLoadSize) {
      return LoadType.DICTIONARY;
    }
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    int start = columnBinary.binaryStart + (Double.BYTES * 2);
    int length = columnBinary.binaryLength - (Double.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);

    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int valueBinaryLength = binary.length - META_LENGTH - nullIndexLength;

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullIndexLength);

    IReadSupporter valueReader =
        ByteBufferSupporterFactory.createReadSupporter(
            binary, META_LENGTH + nullIndexLength, valueBinaryLength, order);
    int index = 0;
    for (; index < startIndex; index++) {
      loader.setNull(index);
    }
    for (int i = 0; i < isNullArray.length; i++, index++) {
      if (isNullArray[i]) {
        loader.setNull(index);
      } else {
        loader.setDouble(index, valueReader.getDouble());
      }
    }
    // NOTE: null padding up to load size
    for (int i = index; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    int start = columnBinary.binaryStart + (Double.BYTES * 2);
    int length = columnBinary.binaryLength - (Double.BYTES * 2);

    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary = compressor.decompress(columnBinary.binary, start, length);

    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);

    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int valueBinaryLength = binary.length - META_LENGTH - nullIndexLength;

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullIndexLength);

    IReadSupporter valueReader =
        ByteBufferSupporterFactory.createReadSupporter(
            binary, META_LENGTH + nullIndexLength, valueBinaryLength, order);

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int lastIndex = startIndex + isNullArray.length - 1;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
      if (i > lastIndex
          || columnBinary.repetitions[i] == 0
          || i < startIndex
          || isNullArray[i - startIndex]) {
        continue;
      }
      dictionarySize++;
    }
    loader.createDictionary(dictionarySize);

    // NOTE:
    //   Set value to dict: dictionaryIndex, valueReader.getDouble()
    //   Set dictionaryIndex: currentIndex, dictionayIndex
    int currentIndex = 0;
    int dictionaryIndex = 0;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        if (i >= startIndex && i <= lastIndex && !isNullArray[i - startIndex]) {
          valueReader.getDouble();
        }
        continue;
      }
      if (i > lastIndex || i < startIndex || isNullArray[i - startIndex]) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      } else {
        loader.setDoubleToDic(dictionaryIndex, valueReader.getDouble());
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setDictionaryIndex(currentIndex, dictionaryIndex);
          currentIndex++;
        }
        dictionaryIndex++;
      }
    }
  }

  @Override
  public void load(final ColumnBinary columnBinary, final ILoader loader) throws IOException {
    if (columnBinary.isSetLoadSize) {
      if (loader.getLoaderType() != LoadType.DICTIONARY) {
        throw new IOException("Loader type is not DICTIONARY.");
      }
      loadFromExpandColumnBinary(columnBinary, (IDictionaryLoader) loader);
    } else {
      if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
        throw new IOException("Loader type is not SEQUENTIAL.");
      }
      loadFromColumnBinary(columnBinary, (ISequentialLoader) loader);
    }
    loader.finish();
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }
}

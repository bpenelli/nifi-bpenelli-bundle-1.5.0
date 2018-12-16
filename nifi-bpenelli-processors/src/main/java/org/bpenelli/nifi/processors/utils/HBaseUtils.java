package org.bpenelli.nifi.processors.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;

public class HBaseUtils {

    /**************************************************************
    * serialize
    **************************************************************/
    public static <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(value, baos);
        return baos.toByteArray();
    }

    /**************************************************************
    * deserialize
    **************************************************************/
    public static <T> T deserialize(final byte[] value, final Deserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(value);
    }

    /**************************************************************
    * checkAndPut
    **************************************************************/
    public static <K, V> boolean checkAndPut(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final K key, final V value, final V checkValue, 
    		final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

    	final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
    	final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        final byte[] checkBytes = serialize(checkValue, valueSerializer);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

        return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, checkBytes, putColumn);
    }

    /**************************************************************
    * checkAndPut
    **************************************************************/
    public static boolean checkAndPut(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final String key, final String value, final String checkValue) throws IOException {

    	final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
    	final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = hbaseService.toBytes(key);
        final byte[] valueBytes = hbaseService.toBytes(value);
        final byte[] checkBytes = hbaseService.toBytes(checkValue);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

        return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, checkBytes, putColumn);
    }

    /**************************************************************
    * putIfAbsent
    **************************************************************/
    public static <K, V> boolean putIfAbsent(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final K key, final V value, 
    		final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

		final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
		final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
		final byte[] rowIdBytes = serialize(key, keySerializer);
		final byte[] valueBytes = serialize(value, valueSerializer);
		final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

		return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, null, putColumn);
    }

    /**************************************************************
    * put
    **************************************************************/
    public static <K, V> void put(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final K key, final V value, 
    		final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

		final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
		final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
		List<PutColumn> putColumns = new ArrayList<PutColumn>(1);
		final byte[] rowIdBytes = serialize(key, keySerializer);
		final byte[] valueBytes = serialize(value, valueSerializer);
		final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);
		putColumns.add(putColumn);

        hbaseService.put(tableName, rowIdBytes, putColumns);
    }

    /**************************************************************
    * put
    **************************************************************/
    public static void put(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final String key, final String value) throws IOException {

		final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
		final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
		List<PutColumn> putColumns = new ArrayList<PutColumn>(1);
		final byte[] rowIdBytes = hbaseService.toBytes(key);
		final byte[] valueBytes = hbaseService.toBytes(value);
		final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);
		putColumns.add(putColumn);

        hbaseService.put(tableName, rowIdBytes, putColumns);
    }

    /**************************************************************
    * containsKey
    **************************************************************/
    public static <K> boolean containsKey(final HBaseClientService hbaseService, final String tableName, 
    		final K key, final Serializer<K> keySerializer) throws IOException {

    	final byte[] rowIdBytes = serialize(key, keySerializer);
		final HBaseLastValueAsBytesRowHandler handler = new HBaseLastValueAsBytesRowHandler();
		final List<Column> columnsList = new ArrayList<Column>(0);
		
		hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
		return (handler.numRows() > 0);
    }

    /**************************************************************
    * containsKey
    **************************************************************/
    public static boolean containsKey(final HBaseClientService hbaseService, final String tableName, final String key) throws IOException {

    	final byte[] rowIdBytes = hbaseService.toBytes(key);
		final HBaseLastValueAsBytesRowHandler handler = new HBaseLastValueAsBytesRowHandler();
		final List<Column> columnsList = new ArrayList<Column>(0);
		hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
		return (handler.numRows() > 0);
    }

    /**************************************************************
     *  getAndPutIfAbsent
     **************************************************************
     *  Note that the implementation of getAndPutIfAbsent is not 
     *  atomic. The putIfAbsent is atomic, but a getAndPutIfAbsent
     *  does a get and then a putIfAbsent. If there is an existing
     *  value and it is updated in between the two steps, then the
     *  existing (unmodified) value will be returned. If the
     *  existing value was deleted between the two steps,
     *  getAndPutIfAbsent will correctly return null.
    **************************************************************/    
    public static <K, V> V getAndPutIfAbsent(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final K key, final V value, 
    		final Serializer<K> keySerializer, final Serializer<V> valueSerializer, 
    		final Deserializer<V> valueDeserializer) throws IOException {
    	
		// Between the get and the putIfAbsent, the value could be deleted or updated.
		// Logic below takes care of the deleted case but not the updated case.
		final V got = HBaseUtils.get(hbaseService, tableName, columnFamily, columnQualifier, key, keySerializer, valueDeserializer);
		final boolean wasAbsent = HBaseUtils.putIfAbsent(hbaseService, tableName, columnFamily, columnQualifier, key, value, keySerializer, valueSerializer);
		
		if (! wasAbsent) return got; else return null;
	}

    /**************************************************************
    * get
    **************************************************************/
    public static <K, V> V get(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final K key, 
    		final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {

		final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
		final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
    	final byte[] rowIdBytes = serialize(key, keySerializer);
    	final HBaseValueAsBytesRowHandler handler = new HBaseValueAsBytesRowHandler();

    	final List<Column> columnsList = new ArrayList<Column>(0);
    	Column col = new Column(columnFamilyBytes, columnQualifierBytes);
    	columnsList.add(col);
            
    	hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for key");
		} else if(handler.numRows() == 1) {
		    return deserialize(handler.getRows().entrySet().iterator().next().getValue().get(columnFamily).get(columnQualifier), valueDeserializer);
		} else {
		    return null;
		}
    }

    /**************************************************************
    * get
    **************************************************************/
    public static String get(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final String key) throws IOException {

    	final byte[] rowIdBytes = hbaseService.toBytes(key);
    	final List<Column> columnsList = new ArrayList<Column>(0);
    	final HBaseValueAsStringRowHandler handler = new HBaseValueAsStringRowHandler();
            
    	hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
		} else if(handler.numRows() == 1) {
			return handler.getRows().entrySet().iterator().next().getValue().get(columnFamily).get(columnQualifier);
		} else {
		    return null;
		}
    }

    /**************************************************************
    * getLastValue
    **************************************************************/
    public static <K, V> V getLastValue(final HBaseClientService hbaseService, final String tableName, final K key, 
    		final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {

    	final byte[] rowIdBytes = serialize(key, keySerializer);
    	final HBaseLastValueAsBytesRowHandler handler = new HBaseLastValueAsBytesRowHandler();

    	final List<Column> columnsList = new ArrayList<Column>(0);
            
    	hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for key");
		} else if(handler.numRows() == 1) {
		    return deserialize(handler.getLastValueBytes(), valueDeserializer);
		} else {
		    return null;
		}
    }

    /**************************************************************
    * getLastValue
    **************************************************************/
    public static String getLastValue(final HBaseClientService hbaseService, final String tableName, final String key) throws IOException {

    	final byte[] rowIdBytes = serialize(key, FlowUtils.stringSerializer);
    	final HBaseLastValueAsStringRowHandler handler = new HBaseLastValueAsStringRowHandler();

    	final List<Column> columnsList = new ArrayList<Column>(0);
            
    	hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for key");
		} else if(handler.numRows() == 1) {
			return handler.getLastValue();
		} else {
		    return null;
		}
    }

    /**************************************************************
    * getByFilter
    **************************************************************/
    public static String getByFilter(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final String filterExpression) throws IOException {

    	final List<Column> columnsList = new ArrayList<Column>(0);
    	final long minTime = 0;
    	final HBaseValueAsStringRowHandler handler = new HBaseValueAsStringRowHandler();
            
    	hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
		} else if(handler.numRows() == 1) {
			return handler.getRows().entrySet().iterator().next().getValue().get(columnFamily).get(columnQualifier);
		} else {
		    return null;
		}
    }
    
    /**************************************************************
    * getLastByFilter
    **************************************************************/
    public static String getLastValueByFilter(final HBaseClientService hbaseService, final String tableName, final String filterExpression) throws IOException {

    	final List<Column> columnsList = new ArrayList<Column>(0);
    	final long minTime = 0;
    	final HBaseLastValueAsStringRowHandler handler = new HBaseLastValueAsStringRowHandler();
            
    	hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);

    	if (handler.numRows() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
		} else if(handler.numRows() == 1) {
			return handler.getLastValue();
		} else {
		    return null;
		}
    }

    /**************************************************************
    * remove
    **************************************************************/
    public static <K> boolean remove(final HBaseClientService hbaseService, final String tableName, 
    		final K key, final Serializer<K> keySerializer) throws IOException {
    	
        final boolean contains = HBaseUtils.containsKey(hbaseService, tableName, key, keySerializer);
        if (contains) {
            final byte[] rowIdBytes = serialize(key, keySerializer);
            hbaseService.delete(tableName, rowIdBytes);
        }
        return contains;
    }
}

final class HBaseValueAsStringRowHandler implements ResultHandler {
    private Map<String, Map<String, Map<String, String>>> rows = new HashMap<String, Map<String, Map<String, String>>>();
    private int numRows = 0;

    @Override
    public void handle(byte[] row, ResultCell[] resultCells) {
    	numRows += 1;
    	final String rowKey = new String(row);
        for( final ResultCell resultCell : resultCells ){
        	final Map<String, Map<String, String>> familyMap = new HashMap<String, Map<String, String>>();
        	final Map<String, String> qualMap = new HashMap<String, String>();
        	final String family = new String(Arrays.copyOfRange(resultCell.getFamilyArray(), resultCell.getFamilyOffset(), resultCell.getFamilyLength() + resultCell.getFamilyOffset()));
        	final String qualifier = new String(Arrays.copyOfRange(resultCell.getQualifierArray(), resultCell.getQualifierOffset(), resultCell.getQualifierLength() + resultCell.getQualifierOffset()));
        	final String value = new String(Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset()));
        	qualMap.put(qualifier, value);
        	familyMap.put(family, qualMap);
        	rows.put(rowKey, familyMap);
        }
    }

    public int numRows() {
        return numRows;
    }

	public Map<String, Map<String, Map<String, String>>> getRows() {
    	return rows;
    }
}

final class HBaseValueAsBytesRowHandler implements ResultHandler {

	private Map<String, Map<String, Map<String, byte[]>>> rows = new HashMap<String, Map<String, Map<String, byte[]>>>();
    private int numRows = 0;

    @Override
    public void handle(byte[] row, ResultCell[] resultCells) {
    	numRows += 1;
    	final String rowKey = new String(row);
        for( final ResultCell resultCell : resultCells ){
        	final Map<String, Map<String, byte[]>> familyMap = new HashMap<String, Map<String, byte[]>>();
        	final Map<String, byte[]> qualMap = new HashMap<String, byte[]>();
        	final String family = new String(Arrays.copyOfRange(resultCell.getFamilyArray(), resultCell.getFamilyOffset(), resultCell.getFamilyLength() + resultCell.getFamilyOffset()));
        	final String qualifier = new String(Arrays.copyOfRange(resultCell.getQualifierArray(), resultCell.getQualifierOffset(), resultCell.getQualifierLength() + resultCell.getQualifierOffset()));
        	final byte[] value = Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset());
        	qualMap.put(qualifier, value);
        	familyMap.put(family, qualMap);
        	rows.put(rowKey, familyMap);
        }
    }

    public int numRows() {
        return numRows;
    }

	public Map<String, Map<String, Map<String, byte[]>>> getRows() {
    	return rows;
    }
}

final class HBaseLastValueAsBytesRowHandler implements ResultHandler {
	private int numRows = 0;
    private byte[] lastValueBytes;

    @Override
    public void handle(byte[] row, ResultCell[] resultCells) {
    	numRows += 1;
    	final int numCells = resultCells.length;
        if (numCells > 0) {
	        final ResultCell resultCell = resultCells[numCells - 1];
	        lastValueBytes = Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset());
        }
    }
    public int numRows() {
        return numRows;
    }
    public byte[] getLastValueBytes() {
       return lastValueBytes;
    }
}

final class HBaseLastValueAsStringRowHandler implements ResultHandler {
	private int numRows = 0;
    private String lastValue;

    @Override
    public void handle(byte[] row, ResultCell[] resultCells) {
    	numRows += 1;
    	final int numCells = resultCells.length;
        if (numCells > 0) {
	        final ResultCell resultCell = resultCells[numCells - 1];
	        lastValue = new String(Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset()));
        }
    }
    public int numRows() {
        return numRows;
    }
    public String getLastValue() {
       return lastValue;
    }
}
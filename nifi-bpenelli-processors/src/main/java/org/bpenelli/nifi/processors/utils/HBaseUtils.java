package org.bpenelli.nifi.processors.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.scan.Column;

@SuppressWarnings({"WeakerAccess", "unused"})
public class HBaseUtils {

	private HBaseUtils() {}

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
		List<PutColumn> putColumns = new ArrayList<>(1);
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
		List<PutColumn> putColumns = new ArrayList<>(1);
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
		final HBaseResultRowHandler handler = new HBaseResultRowHandler();
		final List<Column> columnsList = new ArrayList<>(0);
		
		hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
		return (handler.getResults().rowList.size() > 0);
    }

    /**************************************************************
    * containsKey
    **************************************************************/
    public static boolean containsKey(final HBaseClientService hbaseService, final String tableName, final String key) throws IOException {

    	final byte[] rowIdBytes = hbaseService.toBytes(key);
		final HBaseResultRowHandler handler = new HBaseResultRowHandler();
		final List<Column> columnsList = new ArrayList<>(0);
		hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
		return (handler.getResults().rowList.size() > 0);
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
		final V gotValue = HBaseUtils.get(hbaseService, tableName, columnFamily, columnQualifier, key, keySerializer, valueDeserializer);
		final boolean wasAbsent = HBaseUtils.putIfAbsent(hbaseService, tableName, columnFamily, columnQualifier, key, value, keySerializer, valueSerializer);
		
		if (! wasAbsent) return gotValue; else return null;
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
		final HBaseResultRowHandler handler = new HBaseResultRowHandler();

    	final List<Column> columnsList = new ArrayList<>(0);
    	Column col = new Column(columnFamilyBytes, columnQualifierBytes);
    	columnsList.add(col);
		hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
		HBaseResults results = handler.getResults();

    	if (results.rowList.size() > 1) {
		    throw new IOException("Found multiple rows in HBase for key");
		} else if(results.rowList.size() == 1) {
		    return deserialize(results.rowList.get(0).getCellValueBytes(columnFamily, columnQualifier), valueDeserializer);
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
    	final List<Column> columnsList = new ArrayList<>(0);
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
            
    	hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
        HBaseResults results = handler.getResults();

        if (results.rowList.size() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
		} else if(results.rowList.size() == 1) {
			return results.rowList.get(0).getCellValue(columnFamily, columnQualifier);
		} else {
		    return null;
		}
    }

    /**************************************************************
    * getByFilter
    **************************************************************/
    public static String getByFilter(final HBaseClientService hbaseService, final String tableName, 
    		final String columnFamily, final String columnQualifier, final String filterExpression) throws IOException {

    	final List<Column> columnsList = new ArrayList<>(0);
    	final long minTime = 0;
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
            
    	hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        HBaseResults results = handler.getResults();

        if (results.rowList.size() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
        } else if(results.rowList.size() == 1) {
			return results.rowList.get(0).getCellValue(columnFamily, columnQualifier);
		} else {
		    return null;
		}
    }
    
    /**************************************************************
    * getLastCellValueByFilter
    **************************************************************/
    public static String getLastCellValueByFilter(final HBaseClientService hbaseService, final String tableName,
            final String filterExpression) throws IOException {

    	final List<Column> columnsList = new ArrayList<>(0);
    	final long minTime = 0;
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
            
    	hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        HBaseResults results = handler.getResults();

        if (results.rowList.size() > 1) {
		    throw new IOException("Found multiple rows in HBase for filter expression.");
        } else if(results.rowList.size() == 1) {
			return results.lastCellValue;
		} else {
		    return null;
		}
    }

    /**************************************************************
    * scan
    **************************************************************/
    public static HBaseResults scan(final HBaseClientService hbaseService, final String tableName, 
    		final String filterExpression) throws IOException {

    	final List<Column> columnsList = new ArrayList<>(0);
    	final long minTime = 0;
    	final HBaseResultRowHandler handler = new HBaseResultRowHandler();

    	hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
    	HBaseResults results = handler.getResults();
    	results.tableName = tableName;
    	return results;
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
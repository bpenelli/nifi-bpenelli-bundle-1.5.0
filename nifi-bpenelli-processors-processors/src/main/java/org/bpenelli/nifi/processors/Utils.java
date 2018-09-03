package org.bpenelli.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;

public final class Utils {
	
	private Utils() {

	}

	/**************************************************************
    * applyCase
    **************************************************************/
    public final static String applyCase(final String stringVal, final String toCase) {
        switch (toCase) {
            case "Upper": return stringVal.toUpperCase(); 
            case "Lower": return stringVal.toLowerCase();
            default: return stringVal;
        }
    }

    /**************************************************************
    * applyColMap
    **************************************************************/
    public final static String applyColMap(final ProcessContext context, final FlowFile flowFile, final String sourceTableName, 
    		final String sourceColName, final String toCase) {
        final String colMapKey = sourceTableName + "." + sourceColName;
        String colName = sourceColName;
        for (PropertyDescriptor p : context.getProperties().keySet()) {
            if (p.isDynamic() && p.getName() == colMapKey) {
                PropertyValue propVal = context.getProperty(p);
                colName = propVal.evaluateAttributeExpressions(flowFile).getValue();
                break;
            }
        }
        colName = applyCase(colName, toCase);
        return colName;
    }
    
    public final static String getColValue(final Object col, final String defaultValue) throws SQLException, IOException {
        String result = "";
    	if (col instanceof Clob) {
            Reader stream = ((Clob)col).getCharacterStream();
            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer);
            result = writer.toString();
        } else {
            result = col != null ? col.toString() : defaultValue;
        }
    	return result;
    }
    
    public final static Serializer<String> stringSerializer = new Serializer<String>() {
    	@Override
    	public void serialize(String stringValue, OutputStream out)
    			throws SerializationException, IOException {
    		out.write(stringValue.getBytes(StandardCharsets.UTF_8));
    	}
	};
    
    public final static Deserializer<String> stringDeserializer = new Deserializer<String>() {
    	@Override
    	public String deserialize(byte[] bytes) throws DeserializationException, IOException {
    		return new String(bytes);
    	}	                        	
	};
}

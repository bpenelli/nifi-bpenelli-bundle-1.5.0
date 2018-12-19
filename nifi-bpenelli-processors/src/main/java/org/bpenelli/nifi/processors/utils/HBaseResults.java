package org.bpenelli.nifi.processors.utils;

import java.util.ArrayList;
import java.util.UUID;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class HBaseResults {
	public String rowKeyName = "row_key";
	public String tableName = null;
	public String lastCellValue = null;
	public byte[] lastCellValueBytes = null;
	public final ArrayList<HBaseResultRow> rowList = new ArrayList<>();
    public static final String FMT_TBL_FAM_QUAL = "tablename.family.qualifier";
    public static final String FMT_TBL_QUAL = "tablename.qualifier";
    public static final String FMT_FAM_QUAL = "family.qualifier";
    public static final String FMT_QUAL = "qualifier";
    public String emitFormat = FMT_QUAL;

	/**************************************************************
	 * setLastCellValue
	 **************************************************************/
	public void setLastCellValue(byte[] bytes) {
		this.lastCellValueBytes = bytes;
		this.lastCellValue = new String(bytes);
	}

    /**************************************************************
    * emitFlowFiles
    **************************************************************/
	public void emitFlowFiles(ProcessSession session, FlowFile flowFile, Relationship successRel) {

		final int fragCount = this.rowList.size();
        int fragIndex = 0;
        String fragID = UUID.randomUUID().toString();

        // Iterate the result rows.
        for (HBaseResultRow row : this.rowList) {
        	FlowFile newFlowFile = session.create(flowFile);
        	// Add the row key attribute.
        	newFlowFile = session.putAttribute(newFlowFile, rowKeyName, row.rowKey);
        	fragIndex++;
        	// Iterate the result row cells.
        	for (HBaseResultCell cell : row.cellList) {
            	StringBuilder attrName = new StringBuilder();
            	switch (emitFormat) {
            		case FMT_TBL_FAM_QUAL:
            			attrName.append(this.tableName);
            			attrName.append(".");
            			attrName.append(cell.family);
            			attrName.append(".");
            			attrName.append(cell.qualifier);
            			break;
            		case FMT_TBL_QUAL:
            			attrName.append(this.tableName);
            			attrName.append(".");
            			attrName.append(cell.qualifier);
            			break;	            			
            		case FMT_FAM_QUAL:
            			attrName.append(cell.family);
            			attrName.append(".");
            			attrName.append(cell.qualifier);
            			break;	            			
            		default:
            			attrName.append(cell.qualifier);
            			break;	            			
            	}
            	// Add an attribute to the new FlowFile for the cell.
            	newFlowFile = session.putAttribute(newFlowFile, attrName.toString(), cell.value);
        	}
        	// Add fragment attributes to the new FlowFile.
            newFlowFile = session.putAttribute(newFlowFile, "fragment.identifier", fragID);
            newFlowFile = session.putAttribute(newFlowFile, "fragment.index", Integer.toString(fragIndex));
            newFlowFile = session.putAttribute(newFlowFile, "fragment.count", Integer.toString(fragCount));
            session.transfer(newFlowFile, successRel);
        }
	}
}

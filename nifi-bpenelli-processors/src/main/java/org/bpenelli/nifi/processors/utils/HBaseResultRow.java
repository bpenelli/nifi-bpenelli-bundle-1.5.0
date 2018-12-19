package org.bpenelli.nifi.processors.utils;

import java.util.ArrayList;

public final class HBaseResultRow {
	public String rowKey;
	public byte[] rowKeyBytes;
	public ArrayList<HBaseResultCell> cellList = new ArrayList<>();

	/**************************************************************
	 * setRowKey
	 **************************************************************/
	public void setRowKey(byte[] bytes) {
		this.rowKeyBytes = bytes;
		this.rowKey = new String(bytes);
	}

	/**************************************************************
	 * getCellValue
	 **************************************************************/
	public String getCellValue(String family, String qualifier) {
		for (HBaseResultCell cell : this.cellList) {
			if (cell.family.equals(family) && cell.qualifier.equals(qualifier)) {
				return cell.value;
			}
		}
		return null;
	}

    /**************************************************************
     * getCellValueBytes
     **************************************************************/
    public byte[] getCellValueBytes(String family, String qualifier) {
        for (HBaseResultCell cell : this.cellList) {
            if (cell.family.equals(family) && cell.qualifier.equals(qualifier)) {
                return cell.valueBytes;
            }
        }
        return null;
    }
}

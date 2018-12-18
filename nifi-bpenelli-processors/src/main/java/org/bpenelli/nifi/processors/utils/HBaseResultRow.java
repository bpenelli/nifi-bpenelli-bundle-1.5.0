package org.bpenelli.nifi.processors.utils;

import java.util.ArrayList;

public final class HBaseResultRow {
	public String rowKey;
	public byte[] rowKeyBytes;
	public ArrayList<HBaseResultCell> cellList = new ArrayList<>();
	public void setRowKey(byte[] bytes) {
		this.rowKeyBytes = bytes;
		this.rowKey = new String(bytes);
	}
}
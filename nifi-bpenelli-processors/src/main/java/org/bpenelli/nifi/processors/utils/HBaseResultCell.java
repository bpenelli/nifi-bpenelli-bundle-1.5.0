package org.bpenelli.nifi.processors.utils;

public final class HBaseResultCell {
	public String family;
	public byte[] familyBytes;
	public String qualifier;
	public byte[] nameBytes;
	public String value;
	public byte[] valueBytes;

	/**************************************************************
	 * setFamily
	 **************************************************************/
	public void setFamily(byte[] bytes) {
		this.familyBytes = bytes;
		this.family = new String(bytes);
	}

	/**************************************************************
	 * setQualifier
	 **************************************************************/
	public void setQualifier(byte[] bytes) {
		this.nameBytes = bytes;
		this.qualifier = new String(bytes);
	}

	/**************************************************************
	 * setValue
	 **************************************************************/
	public void setValue(byte[] bytes) {
		this.valueBytes = bytes;
		this.value = new String(bytes);
	}
}
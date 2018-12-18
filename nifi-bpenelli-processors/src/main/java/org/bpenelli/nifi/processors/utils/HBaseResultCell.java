package org.bpenelli.nifi.processors.utils;

public final class HBaseResultCell {
	public String family;
	public byte[] familyBytes;
	public String name;
	public byte[] nameBytes;
	public String value;
	public byte[] valueBytes;
	public void setFamily(byte[] bytes) {
		this.familyBytes = bytes;
		this.family = new String(bytes);
	}
	public void setName(byte[] bytes) {
		this.nameBytes = bytes;
		this.name = new String(bytes);
	}
	public void setValue(byte[] bytes) {
		this.valueBytes = bytes;
		this.value = new String(bytes);
	}
}
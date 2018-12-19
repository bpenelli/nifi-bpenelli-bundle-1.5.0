package org.bpenelli.nifi.processors.utils;

import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;

import java.util.Arrays;

public final class HBaseResultRowHandler implements ResultHandler {
    private final HBaseResults results = new HBaseResults();
    private int rowCount = 0;

    @Override
    public void handle(byte[] resultRow, ResultCell[] resultCells) {
        rowCount += 1;
        HBaseResultRow row = new HBaseResultRow();
        row.setRowKey(resultRow);
        for( final ResultCell resultCell : resultCells ){
            HBaseResultCell cell = new HBaseResultCell();
            cell.setFamily(Arrays.copyOfRange(resultCell.getFamilyArray(), resultCell.getFamilyOffset(), resultCell.getFamilyLength() + resultCell.getFamilyOffset()));
            cell.setQualifier(Arrays.copyOfRange(resultCell.getQualifierArray(), resultCell.getQualifierOffset(), resultCell.getQualifierLength() + resultCell.getQualifierOffset()));
            cell.setValue(Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset()));
            row.cellList.add(cell);
            results.setLastCellValue(cell.valueBytes);
        }
        results.rowList.add(row);
    }

    public int getRowCount() {
        return rowCount;
    }

    public HBaseResults getResults() {
        return results;
    }
}

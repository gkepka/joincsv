package app.joins;

import app.utils.CSVUtil;
import app.utils.JoinType;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.file.Path;

public abstract class JoinUtil {
    protected final Path leftCSV;
    protected final Path rightCSV;
    protected final String joinColumn;
    protected final JoinType joinType;

    protected JoinUtil(Path leftCSV, Path rightCSV, String joinColumn, JoinType joinType) {
        this.leftCSV = leftCSV;
        this.rightCSV = rightCSV;
        this.joinColumn = joinColumn;
        this.joinType = joinType;
    }

    public abstract void join(boolean printHeader) throws IOException, CsvException;

    private int getIndexOfColumn(String[] header) {
        for (int i = 0; i < header.length; i++) {
            if (joinColumn.equals(header[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Header does not contain specified column name");
    }

    protected String[] getMatchedRows(String[] leftRow, String[] rightRow, int leftIndex, int rightIndex) {
        int totalLength = leftRow.length + rightRow.length - 1;
        if (leftRow[leftIndex].equals(rightRow[rightIndex])) {
            return combineRows(leftRow, rightRow, leftIndex, rightIndex, totalLength);
        } else {
            switch (joinType) {
                case LEFT:
                    return combineRows(leftRow, null, leftIndex, rightIndex, totalLength);
                case RIGHT:
                    return combineRows(null, rightRow, leftIndex, rightIndex, totalLength);
                default:
                    return null;
            }
        }
    }

    protected int getIndexOfJoinColumn(Path file, String joinColumn) throws IOException, CsvException {
        try (CSVUtil csvUtil = new CSVUtil(file, false)){
            String[] header = csvUtil.readRow();
            return getIndexOfColumn(header);
        }
    }

    protected void printHeader() throws IOException, CsvException {
        try (CSVUtil leftReader = new CSVUtil(leftCSV, false);
             CSVUtil rightReader = new CSVUtil(rightCSV, false))
        {
            String[] leftHeader = leftReader.readRow();
            String[] rightHeader = rightReader.readRow();

            int leftIndex = getIndexOfColumn(leftHeader);
            int rightIndex = getIndexOfColumn(rightHeader);
            leftReader.printRowToStdout(combineHeaders(leftHeader, rightHeader, leftIndex, rightIndex));
        }

    }

    protected String[] combineHeaders(String[] leftHeader, String[] rightHeader, int leftIndex, int rightIndex) {
        String[] header = new String[leftHeader.length + rightHeader.length - 1];
        int index = 0;
        for (int i = 0; i < leftHeader.length; i++) {
            if (i != leftIndex) {
                header[index++] = leftHeader[i];
            }
        }
        header[index++] = joinColumn;
        for (int i = 0; i < rightHeader.length; i++) {
            if (i != rightIndex) {
                header[index++] = rightHeader[i];
            }
        }
        return header;
    }

    protected String[] combineRows(String[] leftRow, String[] rightRow, int leftIndex, int rightIndex, int totalLength) {
        if (leftRow == null && rightRow == null) {
            throw new IllegalArgumentException("Both rows cannot be null");
        }
        String[] row = new String[totalLength];
        int index = 0;

        if (leftRow == null) {
            for (int i = 0; i < totalLength - rightRow.length; i++) {
                row[index++] = null;
            }
        } else {
            for (int i = 0; i < leftRow.length; i++) {
                if (i != leftIndex) {
                    row[index++] = leftRow[i];
                }
            }
        }

        row[index++] = leftRow == null ? rightRow[rightIndex] : leftRow[leftIndex];

        if (rightRow == null) {
            for (int i = 0; i < totalLength - leftRow.length; i++) {
                row[index++] = null;
            }
        } else {
            for (int i = 0; i < rightRow.length; i++) {
                if (i != rightIndex) {
                    row[index++] = rightRow[i];
                }
            }
        }

        return row;
    }
}

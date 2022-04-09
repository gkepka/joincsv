package app.utils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopJoinUtil {
    private final File leftCSV;
    private final File rightCSV;
    private final String joinColumn;
    private final JoinType joinType;

    public NestedLoopJoinUtil(File leftCSV, File rightCSV, String joinColumn, JoinType joinType) {
        this.leftCSV = leftCSV;
        this.rightCSV = rightCSV;
        this.joinColumn = joinColumn;
        this.joinType = joinType;
    }

    private List<String[]> readCSVFile(File file) throws IOException, CsvException {
        CSVReader reader = new CSVReader(new BufferedReader(new FileReader(file)));
        List<String[]> rows = reader.readAll();
        reader.close();
        return rows;
    }

    private void printCSVFile(List<String[]> rows) throws IOException {
        CSVWriter writer = new CSVWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
        writer.writeAll(rows);
        writer.close();
    }
    
    private int getIndexOfColumn(String[] header) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].equals(joinColumn)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Header does not contain specified column name");
    }

    private String[] combineHeaders(String[] leftHeader, String[] rightHeader, int leftIndex, int rightIndex) {
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

    private String[] combineRows(String[] leftRow, String[] rightRow, int leftIndex, int rightIndex, int totalLength) {
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
    
    public void join() throws IOException, CsvException {
        List<String[]> leftRows = readCSVFile(leftCSV);
        List<String[]> rightRows = readCSVFile(rightCSV);

        String[] leftHeader = leftRows.remove(0);
        String[] rightHeader = rightRows.remove(0);

        int leftIndex = getIndexOfColumn(leftHeader);
        int rightIndex = getIndexOfColumn(rightHeader);

        List<String[]> outputRows = new ArrayList<>();

        String[] combinedHeader = combineHeaders(leftHeader, rightHeader, leftIndex, rightIndex);
        int totalLength =combinedHeader.length;

        outputRows.add(combinedHeader);

        for (String[] leftRow : leftRows) {
            for (String[] rightRow : rightRows) {
                if (leftRow[leftIndex].equals(rightRow[rightIndex])) {
                    outputRows.add(combineRows(leftRow, rightRow, leftIndex, rightIndex, totalLength));
                } else {
                    switch (joinType) {
                        case LEFT -> outputRows.add(combineRows(leftRow, null, leftIndex, rightIndex, totalLength));
                        case RIGHT -> outputRows.add(combineRows(null, rightRow, leftIndex, rightIndex, totalLength));
                    }
                }
            }
        }
        printCSVFile(outputRows);
    }
}

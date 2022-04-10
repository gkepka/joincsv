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

    protected int getRowLength(Path path) throws IOException, CsvException {
        CSVUtil csvUtil = new CSVUtil(path, false);
        return csvUtil.readRow().length;
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

            leftReader.printRowToStdout(combineRows(leftHeader, rightHeader, leftHeader.length + rightHeader.length));
        }
    }

    protected String[] combineRows(String[] leftRow, String[] rightRow, int totalLength) {
        if (leftRow == null && rightRow == null) {
            throw new IllegalArgumentException("Both rows cannot be null");
        }

        String[] output = new String[totalLength];
        int index = 0;

        if (leftRow == null) {
            index = totalLength - rightRow.length;
            for (String value : rightRow) {
                output[index++] = value;
            }
        } else if (rightRow == null) {
          for (String value : leftRow) {
              output[index++] = value;
          }
        } else {
            for (String value : leftRow) {
                output[index++] = value;
            }
            for (String value : rightRow) {
                output[index++] = value;
            }
        }
        return output;
    }
}

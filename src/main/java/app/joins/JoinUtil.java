package app.joins;

import app.utils.CSVUtil;
import app.utils.RowUtil;
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

    protected int getRowLength(Path path) throws IOException, CsvException {
        try (CSVUtil csvUtil = new CSVUtil(path, false)) {
            return csvUtil.readRow().length;
        }
    }

    protected int getIndexOfJoinColumn(Path file) throws IOException, CsvException {
        try (CSVUtil csvUtil = new CSVUtil(file, false)){
            String[] header = csvUtil.readRow();
            return RowUtil.getIndexOfColumn(header, joinColumn);
        }
    }

    protected void printHeader() throws IOException, CsvException {
        try (CSVUtil leftReader = new CSVUtil(leftCSV, false);
             CSVUtil rightReader = new CSVUtil(rightCSV, false))
        {
            String[] leftHeader = leftReader.readRow();
            String[] rightHeader = rightReader.readRow();

            leftReader.printRowToStdout(RowUtil.combineRows(leftHeader, rightHeader, leftHeader.length + rightHeader.length));
        }
    }

}

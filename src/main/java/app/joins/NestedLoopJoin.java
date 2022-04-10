package app.joins;

import app.utils.CSVUtil;
import app.utils.JoinType;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopJoin extends JoinUtil {

    public NestedLoopJoin(Path leftCSV, Path rightCSV, String joinColumn, JoinType joinType) {
        super(leftCSV, rightCSV, joinColumn, joinType);
    }

    @Override
    public void join(boolean printHeader) throws IOException, CsvException {
        CSVUtil leftReader = new CSVUtil(leftCSV, false);
        CSVUtil rightReader = new CSVUtil(rightCSV, false);
        List<String[]> leftRows = leftReader.readCSVFile();
        List<String[]> rightRows = rightReader.readCSVFile();

        String[] leftHeader = leftRows.remove(0);
        String[] rightHeader = rightRows.remove(0);

        int leftIndex = getIndexOfJoinColumn(leftCSV, joinColumn);
        int rightIndex = getIndexOfJoinColumn(rightCSV, joinColumn);

        List<String[]> outputRows = new ArrayList<>();

        String[] combinedHeader = combineHeaders(leftHeader, rightHeader, leftIndex, rightIndex);

        if (printHeader) {
            outputRows.add(combinedHeader);
        }

        for (String[] leftRow : leftRows) {
            for (String[] rightRow : rightRows) {
                String[] outputRow = getMatchedRows(leftRow, rightRow, leftIndex, rightIndex);
                if (outputRow != null) {
                    outputRows.add(outputRow);
                }
            }
        }
        leftReader.printCSVToStdout(outputRows);
        leftReader.close();
        rightReader.close();
    }
}

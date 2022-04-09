package app.utils;

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

        int leftIndex = getIndexOfColumn(leftHeader);
        int rightIndex = getIndexOfColumn(rightHeader);

        List<String[]> outputRows = new ArrayList<>();

        String[] combinedHeader = combineHeaders(leftHeader, rightHeader, leftIndex, rightIndex);
        int totalLength =combinedHeader.length;

        if (printHeader) {
            outputRows.add(combinedHeader);
        }

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
        leftReader.printCSVToStdout(outputRows);
        leftReader.close();
        rightReader.close();
    }
}

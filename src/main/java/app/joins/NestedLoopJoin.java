package app.joins;

import app.utils.CSVUtil;
import app.utils.RowUtil;
import app.utils.RuntimeUtil;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class NestedLoopJoin extends JoinUtil {

    public NestedLoopJoin(Path leftCSV, Path rightCSV, String joinColumn, JoinType joinType) {
        super(leftCSV, rightCSV, joinColumn, joinType);
    }

    @Override
    public void join(boolean printHeader) throws IOException, CsvException {
        try (CSVUtil leftReader = new CSVUtil(leftCSV, false);
             CSVUtil rightReader = new CSVUtil(rightCSV, false))
        {
            int leftIndex = getIndexOfJoinColumn(leftCSV);
            int rightIndex = getIndexOfJoinColumn(rightCSV);

            String[] leftHeader = leftReader.readRow();
            String[] rightHeader = rightReader.readRow();

            if (printHeader) {
                printHeader();
            }

            String[] leftFirstRow = leftReader.readRow();
            long leftRowLength = Arrays.stream(leftFirstRow).filter(Objects::nonNull).map(String::length).reduce(0, Integer::sum);

            long freeMemory = RuntimeUtil.getTotalFreeMemory();
            long leftRowsToLoad = (freeMemory / 4) / leftRowLength;

            String[] rightFirstRow = rightReader.readRow();
            long rightRowLength = Arrays.stream(leftFirstRow).filter(Objects::nonNull).map(String::length).reduce(0, Integer::sum);

            long rightRowsToLoad = (freeMemory / 4) / rightRowLength;


            boolean leftEnded = false;
            boolean rightEnded = false;
            while (!leftEnded) {
                List<String[]> leftRows = leftReader.readRows((int) leftRowsToLoad);
                if (leftRows.size() < leftRowsToLoad) {
                    leftEnded = true;
                    leftRows.add(leftFirstRow);
                }
                while (!rightEnded) {
                    List<String[]> rightRows = rightReader.readRows((int) rightRowsToLoad);
                    if (rightRows.size() < rightRowsToLoad) {
                        rightEnded = true;
                        rightRows.add(rightFirstRow);
                    }
                    if (joinType != JoinType.RIGHT) {
                        joinRows(leftRows, rightRows, leftIndex, rightIndex);
                    } else {
                        joinRows(rightRows, leftRows, rightIndex, leftIndex);
                    }
                }
            }
        }
    }

    private void joinRows(List<String[]> leftRows, List<String[]> rightRows, int leftIndex, int rightIndex) throws IOException {
        try (CSVUtil csvUtil = new CSVUtil(Files.createTempFile("tmp",""), false)) {
            int totalLength = leftRows.get(0).length + rightRows.get(0).length;
            for (String[] leftRow : leftRows) {
                boolean leftMatched = false;
                for (String[] rightRow : rightRows) {
                    if (leftRow[leftIndex].equals(rightRow[rightIndex])) {
                        leftMatched = true;
                        if (joinType != JoinType.RIGHT) {
                            csvUtil.printRowToStdout(RowUtil.combineRows(leftRow, rightRow, totalLength));
                        } else {
                            csvUtil.printRowToStdout(RowUtil.combineRows(rightRow, leftRow, totalLength));
                        }
                    }
                }
                if (joinType != JoinType.INNER && !leftMatched) {
                    if (joinType == JoinType.RIGHT) {
                        csvUtil.printRowToStdout(RowUtil.combineRows(null, leftRow, totalLength));
                    } else if (joinType == JoinType.LEFT) {
                        csvUtil.printRowToStdout(RowUtil.combineRows(leftRow, null, totalLength));
                    }
                }
            }
        }
    }
}
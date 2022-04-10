package app.joins;

import app.utils.CSVUtil;
import app.utils.RowUtil;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class HashJoin extends JoinUtil {

    public HashJoin(Path leftCSV, Path rightCSV, String joinColumn, JoinType joinType) {
        super(leftCSV, rightCSV, joinColumn, joinType);
    }

    @Override
    public void join(boolean printHeader) throws IOException, CsvException {
        if (printHeader) {
            printHeader();
        }

        int leftIndex = getIndexOfJoinColumn(leftCSV);
        int rightIndex = getIndexOfJoinColumn(rightCSV);

        if (joinType == JoinType.RIGHT || joinType == JoinType.INNER) {
            hashJoin(leftCSV, rightCSV, leftIndex, rightIndex);
        } else {
            hashJoin(rightCSV, leftCSV, rightIndex, leftIndex);
        }

    }

    private void hashJoin(Path leftFile, Path rightFile, int leftIndex, int rightIndex) throws IOException, CsvException {
        try (CSVUtil csvUtil = new CSVUtil(rightFile, false)){
            int totalLength = getRowLength(leftFile) + getRowLength(rightFile);
            Multimap<String, String[]> hashMap = createHashMap(leftFile, leftIndex);

            LinkedList<String[]> rightRows = (LinkedList<String[]>) csvUtil.readCSVFile();
            rightRows.removeFirst();

            for (String[] rightRow : rightRows) {
                boolean rightMatched = false;
                Collection<String[]> matches = hashMap.get(rightRow[rightIndex]);
                for (String[] leftRow : matches) {
                    if (leftRow[leftIndex].equals(rightRow[rightIndex])) {
                        rightMatched = true;
                        if (joinType != JoinType.LEFT) {
                            csvUtil.printRowToStdout(RowUtil.combineRows(leftRow, rightRow, totalLength));
                        } else {
                            csvUtil.printRowToStdout(RowUtil.combineRows(rightRow, leftRow, totalLength));
                        }
                    }
                }
                if (joinType != JoinType.INNER && !rightMatched) {
                    if (joinType == JoinType.RIGHT) {
                        csvUtil.printRowToStdout(RowUtil.combineRows(null, rightRow, totalLength));
                    } else if (joinType == JoinType.LEFT) {
                        csvUtil.printRowToStdout(RowUtil.combineRows(rightRow, null, totalLength));
                    }
                }
            }
        }
    }

    private Multimap<String, String[]> createHashMap(Path file, int columnIndex) throws IOException, CsvException {
        try (CSVUtil reader = new CSVUtil(file, false)) {
            LinkedList<String[]> rows = (LinkedList<String[]>) reader.readCSVFile();
            rows.removeFirst();

            if (rows.isEmpty()) {
                return ArrayListMultimap.create();
            }

            long rowLength = Arrays.stream(rows.get(0)).filter(Objects::nonNull).map(String::length).reduce(0, Integer::sum);
            long approxRows = Files.size(file) / rowLength;

            Multimap<String, String[]> hashMap = ArrayListMultimap.create((int) (approxRows * 1.2), 1);

            for (String[] row : rows) {
                hashMap.put(row[columnIndex], row);
            }
            return hashMap;
        }
    }
}

package app.joins;

import app.utils.CSVUtil;
import app.utils.JoinType;
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

        int leftIndex = getIndexOfJoinColumn(leftCSV, joinColumn);
        int rightIndex = getIndexOfJoinColumn(rightCSV, joinColumn);

        int totalLength = getRowLength(leftCSV) + getRowLength(rightCSV);

        Multimap<String, String[]> hashMap;
        CSVUtil csvUtil;
        LinkedList<String[]> rightRows;

        if (joinType == JoinType.RIGHT || joinType == JoinType.INNER) {
            hashMap = createHashMap(leftCSV, leftIndex);
            csvUtil = new CSVUtil(rightCSV, false);
            rightRows = (LinkedList<String[]>) csvUtil.readCSVFile();
        } else { // invert files position
            int tmp = leftIndex;
            leftIndex = rightIndex;
            rightIndex = tmp;
            hashMap = createHashMap(rightCSV, rightIndex);
            csvUtil = new CSVUtil(leftCSV, false);
            rightRows = (LinkedList<String[]>) csvUtil.readCSVFile();
        }
        rightRows.removeFirst();

        for (String[] rightRow : rightRows) {
            boolean rightMatched = false;
            Collection<String[]> matches = hashMap.get(rightRow[rightIndex]);
            for (String[] leftRow : matches) {
                if (leftRow[leftIndex].equals(rightRow[rightIndex])) {
                    rightMatched = true;
                    csvUtil.printRowToStdout(combineRows(leftRow, rightRow, totalLength));
                }
            }
            if (joinType != JoinType.INNER && !rightMatched) {
                if (joinType == JoinType.RIGHT) {
                    csvUtil.printRowToStdout(combineRows(null, rightRow, totalLength));
                } else if (joinType == JoinType.LEFT) {
                    csvUtil.printRowToStdout(combineRows(rightRow, null, totalLength));
                }
            }
        }

        csvUtil.close();
    }

    private Multimap<String, String[]> createHashMap(Path file, int columnIndex) throws IOException, CsvException {
        try (CSVUtil reader = new CSVUtil(file, false)) {
            LinkedList<String[]> rows = (LinkedList<String[]>) reader.readCSVFile();
            rows.removeFirst();

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

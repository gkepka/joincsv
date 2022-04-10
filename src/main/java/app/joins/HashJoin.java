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

        Multimap<String, String[]> hashMap = createHashMap(leftCSV, leftIndex);
        CSVUtil csvUtil = new CSVUtil(rightCSV, false);
        LinkedList<String[]> rightRows = (LinkedList<String[]>) csvUtil.readCSVFile();
        rightRows.removeFirst();

        for (String[] rightRow : rightRows) {
            Collection<String[]> matches = hashMap.get(rightRow[rightIndex]);
            if (matches.isEmpty()) continue;
            for (String[] leftRow : matches) {
                String[] outputRow = getMatchedRows(leftRow, rightRow, leftIndex, rightIndex);
                if (outputRow != null) {
                    csvUtil.printRowToStdout(outputRow);
                }
            }
        }

        csvUtil.close();
    }

    private Multimap<String, String[]> createHashMap(Path file, int columnIndex) throws IOException, CsvException {
        try (CSVUtil reader = new CSVUtil(file, false)) {
            LinkedList<String[]> rows = (LinkedList<String[]>) reader.readCSVFile();
            rows.removeFirst();

            if (rows.isEmpty()) {
                return null;
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

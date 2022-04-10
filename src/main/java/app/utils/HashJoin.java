package app.utils;

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
        CSVUtil leftReader = new CSVUtil(leftCSV, false);
        CSVUtil rightReader = new CSVUtil(rightCSV, false);

        String[] leftHeader = leftReader.readRow();
        String[] rightHeader = rightReader.readRow();

        int leftIndex = getIndexOfColumn(leftHeader);
        int rightIndex = getIndexOfColumn(rightHeader);

        if (printHeader) {
            leftReader.printRowToStdout(combineHeaders(leftHeader, rightHeader, leftIndex, rightIndex));
        }

        Multimap<String, String[]> hashMap = createHashMap(leftCSV, leftIndex);
        List<String[]> outputRows = new ArrayList<>(hashMap.size());
        CSVUtil csvUtil = new CSVUtil(rightCSV, false);
        List<String[]> rightRows = csvUtil.readCSVFile();

        for (String[] rightRow : rightRows) {
            Collection<String[]> matches = hashMap.get(rightRow[rightIndex]);
            if (matches.isEmpty()) continue;
            for (String[] leftRow : matches) {
                String[] outputRow = getMatchedRows(leftRow, rightRow, leftIndex, rightIndex);
                if (outputRow != null) {
                    outputRows.add(outputRow);
                }
            }
        }

        csvUtil.printCSVToStdout(outputRows);
        csvUtil.close();
    }

    private Multimap<String, String[]> createHashMap(Path file, int columnIndex) throws IOException, CsvException {
        try (CSVUtil reader = new CSVUtil(file, false)) {
            List<String[]> rows = reader.readCSVFile();

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

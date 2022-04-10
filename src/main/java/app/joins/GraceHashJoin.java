package app.joins;

import app.utils.CSVUtil;
import app.utils.HashFunction;
import app.utils.JoinType;
import app.utils.RuntimeUtil;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class GraceHashJoin extends JoinUtil {

    private final int FILES_COUNT = 128;

    public GraceHashJoin(Path leftCSV, Path rightCSV, String joinColumn, JoinType joinType) {
        super(leftCSV, rightCSV, joinColumn, joinType);
    }

    @Override
    public void join(boolean printHeader) throws IOException, CsvException {
        if (printHeader) {
            printHeader();
        }

        int leftIndex = getIndexOfJoinColumn(leftCSV, joinColumn);
        int rightIndex = getIndexOfJoinColumn(rightCSV, joinColumn);

        List<Path> leftPartitions = partitionFile(leftCSV, "L", leftIndex, String::hashCode);
        List<Path> rightPartitions = partitionFile(rightCSV, "R", rightIndex, String::hashCode);

        for (int i = 0; i < leftPartitions.size(); i++) {
            Path leftFile = leftPartitions.get(i);
            Path rightFile = rightPartitions.get(i);

            HashJoin hashJoin = new HashJoin(leftFile, rightFile, joinColumn, joinType);
            hashJoin.join(false);

            Files.delete(leftFile);
            Files.delete(rightFile);
        }
    }

    public List<Path> partitionFile(Path path, String prefix, int columnIndex, HashFunction hashFunction) throws IOException, CsvException {
        List<Path> tmpFiles = new ArrayList<>(FILES_COUNT);
        List<CSVUtil> writers = new ArrayList<>(FILES_COUNT);
        try {
            for (int i = 0; i < FILES_COUNT; i++) {
                tmpFiles.add(i, Files.createTempFile(prefix + i + "_", ".csv"));
                tmpFiles.get(i).toFile().deleteOnExit();
                writers.add(new CSVUtil(tmpFiles.get(i), true));
            }

            CSVUtil reader = new CSVUtil(path, false);

            String[] header = reader.readRow();
            for (CSVUtil writer : writers) {
                writer.writeRow(header);
            }

            String[] firstRow = reader.readRow();
            long rowLength = Arrays.stream(firstRow).filter(Objects::nonNull).map(String::length).reduce(0, Integer::sum);

            long freeMemory = RuntimeUtil.getTotalFreeMemory();
            long rowsToLoad = (freeMemory / 2) / rowLength;

            boolean reachedEnd = false;
            while (!reachedEnd) {
                List<String[]> rows = reader.readRows((int) rowsToLoad);
                if (rows.size() < rowsToLoad) {
                    reachedEnd = true;
                    rows.add(firstRow);
                }
                for (String[] row : rows) {
                    String columnValue = row[columnIndex];
                    int hash = hashFunction.getHash(columnValue) % FILES_COUNT;
                    if (hash < 0) hash += FILES_COUNT;
                    writers.get(hash).writeRow(row);
                }
            }
        } finally {
            for (CSVUtil writer : writers) {
                writer.close();
            }
        }

        return tmpFiles;
    }
}

package app.joins;

import app.utils.CSVUtil;
import app.utils.RuntimeUtil;
import com.google.common.hash.Hashing;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

        int leftIndex = getIndexOfJoinColumn(leftCSV);
        int rightIndex = getIndexOfJoinColumn(rightCSV);

        List<Path> leftPartitions = partitionFile(leftCSV, "L_", leftIndex, String::hashCode, FILES_COUNT);
        List<Path> rightPartitions = partitionFile(rightCSV, "R_", rightIndex, String::hashCode, FILES_COUNT);

        for (int i = 0; i < leftPartitions.size(); i++) {
            Path leftFile = leftPartitions.get(i);
            Path rightFile = rightPartitions.get(i);

            long totalFree = RuntimeUtil.getTotalFreeMemory();
            long csvSize = Files.size(leftFile) + Files.size(rightFile);

            if (csvSize > totalFree * 0.8){
                if (leftFile.getFileName().toString().startsWith("L2_") || rightFile.getFileName().toString().startsWith("R2_")) {
                    new NestedLoopJoin(leftFile, rightFile, joinColumn, joinType).join(false);
                } else {
                    HashFunction hashFunction = (s) -> Hashing.murmur3_32_fixed().hashString(s, StandardCharsets.UTF_8).asInt();
                    List<Path> newPartitionsLeft = partitionFile(leftFile, "L2_", leftIndex, hashFunction, 32);
                    List<Path> newPartitionsRight = partitionFile(rightFile, "R2_", rightIndex, hashFunction, 32);
                    leftPartitions.addAll(newPartitionsLeft);
                    rightPartitions.addAll(newPartitionsRight);
                }
            } else {
                HashJoin hashJoin = new HashJoin(leftFile, rightFile, joinColumn, joinType);
                hashJoin.join(false);
            }

            Files.delete(leftFile);
            Files.delete(rightFile);
        }
    }

    private List<Path> partitionFile(Path path, String prefix, int columnIndex, HashFunction hashFunction, int partitions) throws IOException, CsvException {
        List<Path> tmpFiles = new ArrayList<>(partitions);
        List<CSVUtil> writers = new ArrayList<>(partitions);
        try {
            for (int i = 0; i < partitions; i++) {
                tmpFiles.add(i, Files.createTempFile(prefix + i, ".csv"));
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
                    int hash = hashFunction.getHash(columnValue) % partitions;
                    if (hash < 0) hash += partitions;
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

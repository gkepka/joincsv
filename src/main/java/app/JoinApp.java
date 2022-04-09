package app;

import app.utils.GraceHashJoin;
import app.utils.JoinType;
import app.utils.RuntimeUtil;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class JoinApp {
    private final Path leftCSV;
    private final Path rightCSV;
    private final String columnName;
    private final JoinType joinType;


    public JoinApp(Path leftCSV, Path rightCSV, String columnName, JoinType joinType) {
        this.leftCSV = leftCSV;
        this.rightCSV = rightCSV;
        this.columnName = columnName;
        this.joinType = joinType;
    }

    public void executeJoin() throws IOException {

        long totalFree = RuntimeUtil.getTotalFreeMemory();

        long csvSize = Files.size(leftCSV) + Files.size(rightCSV);

//        if (csvSize < totalFree * 0.9){
//            // use in-memory Hash Join
//        } else {
//            // use GRACE Hash Join
//        }

//        NestedLoopJoin nestedLoopJoin = new NestedLoopJoin(leftCSV, rightCSV, columnName, joinType);
//        try {
//            nestedLoopJoin.join();
//        } catch (IOException | CsvException e) {
//            e.printStackTrace();
//        }
        GraceHashJoin graceHashJoin = new GraceHashJoin(leftCSV, rightCSV, columnName, joinType);
        try {
            graceHashJoin.join(true);
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: join file_path file_path column_name join_type");
            return;
        }

        JoinType joinType;
        if (args.length == 3) {
            joinType = JoinType.INNER;
        } else {
            joinType = getJoinType(args[3]);
            if (joinType == JoinType.NONE) {
                System.out.println("Allowed join types: inner (default), left, right");
                return;
            }
        }

        Path leftCSV = Path.of(args[0]);
        Path rightCSV = Path.of(args[1]);

        if (!Files.exists(leftCSV) || Files.isDirectory(leftCSV)) {
            System.out.println("File " + args[0] + " does not exist or is a directory");
            return;
        }
        if (!Files.exists(rightCSV)|| Files.isDirectory(rightCSV)) {
            System.out.println("File " + args[1] + " does not exist or is a directory");
            return;
        }

        JoinApp joinApp = new JoinApp(leftCSV, rightCSV, args[2], joinType);
        try {
            joinApp.executeJoin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JoinType getJoinType(String join_type) {
        return switch (join_type) {
            case "join" -> JoinType.INNER;
            case "left" -> JoinType.LEFT;
            case "right" -> JoinType.RIGHT;
            default -> JoinType.NONE;
        };
    }
}

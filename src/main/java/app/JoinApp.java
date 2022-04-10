package app;

import app.joins.GraceHashJoin;
import app.joins.HashJoin;
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

    public void executeJoin() throws IOException, CsvException {
        long totalFree = RuntimeUtil.getTotalFreeMemory();
        long csvSize = Files.size(leftCSV) + Files.size(rightCSV);

        if (csvSize < totalFree * 0.9){
            HashJoin hashJoin = new HashJoin(leftCSV, rightCSV, columnName, joinType);
            hashJoin.join(true);
        } else {
            GraceHashJoin graceHashJoin = new GraceHashJoin(leftCSV, rightCSV, columnName, joinType);
            graceHashJoin.join(true);
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
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

    private static JoinType getJoinType(String join_type) {
         switch (join_type) {
            case "join":
                return JoinType.INNER;
             case "left":
                 return JoinType.LEFT;
             case "right":
                 return JoinType.RIGHT;
             default:
                 return JoinType.NONE;
        }
    }
}

package app;

import app.utils.JoinType;
import app.utils.NestedLoopJoinUtil;
import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.IOException;

public class JoinApp {
    private final File leftCSV;
    private final File rightCSV;
    private final String columnName;
    private final JoinType joinType;


    public JoinApp(File leftCSV, File rightCSV, String columnName, JoinType joinType) {
        this.leftCSV = leftCSV;
        this.rightCSV = rightCSV;
        this.columnName = columnName;
        this.joinType = joinType;
    }

    public void executeJoin() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long usedMemory = totalMemory - Runtime.getRuntime().freeMemory();
        long totalFree = maxMemory - usedMemory;

        long csvSize = leftCSV.length() + rightCSV.length();

//        if (csvSize < 100_000_000 && csvSize < 0.1 * totalFree) {
//            // use Nested Loop Join
//        } else if (csvSize < totalFree * 0.9){
//            // use in-memory Hash Join
//        } else {
//            // use GRACE Hash Join
//        }

        NestedLoopJoinUtil nestedLoopJoinUtil = new NestedLoopJoinUtil(leftCSV, rightCSV, columnName, joinType);
        try {
            nestedLoopJoinUtil.join();
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

        File leftCSV = new File(args[0]);
        File rightCSV = new File(args[1]);

        if (!leftCSV.exists() || leftCSV.isDirectory()) {
            System.out.println("File " + args[0] + " does not exist or is a directory");
            return;
        }
        if (!rightCSV.exists() || rightCSV.isDirectory()) {
            System.out.println("File " + args[1] + " does not exist or is a directory");
            return;
        }

        JoinApp joinApp = new JoinApp(leftCSV, rightCSV, args[2], joinType);
        joinApp.executeJoin();
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

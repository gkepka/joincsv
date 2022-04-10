package app.utils;

public class RowUtil {

    public static int getIndexOfColumn(String[] header, String column) {
        for (int i = 0; i < header.length; i++) {
            if (column.equals(header[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Header does not contain specified column name");
    }

    public static String[] combineRows(String[] leftRow, String[] rightRow, int totalLength) {
        if (leftRow == null && rightRow == null) {
            throw new IllegalArgumentException("Both rows cannot be null");
        }

        String[] output = new String[totalLength];
        int index = 0;

        if (leftRow == null) {
            index = totalLength - rightRow.length;
            for (String value : rightRow) {
                output[index++] = value;
            }
        } else if (rightRow == null) {
            for (String value : leftRow) {
                output[index++] = value;
            }
        } else {
            for (String value : leftRow) {
                output[index++] = value;
            }
            for (String value : rightRow) {
                output[index++] = value;
            }
        }
        return output;
    }
}

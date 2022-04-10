package app.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RowUtilTest {

    @Test
    void getIndexOfColumn() {
        String[] header = {"column_0", "column_1", "column_2"};
        assertEquals(1, RowUtil.getIndexOfColumn(header,"column_1"));
        assertThrows(IllegalArgumentException.class, () -> RowUtil.getIndexOfColumn(header, "column_10"));
    }

    @Test
    void combineRows() {
        assertThrows(IllegalArgumentException.class, () -> RowUtil.combineRows(null, null, 10));

        String[] row1 = {"column_0", "column_1", "column_2"};
        String[] row2 = {"column_3", "column_4", "column_5"};
        String[] expectedCombined = Stream.concat(Arrays.stream(row1), Arrays.stream(row2)).toArray(String[]::new);

        assertArrayEquals(expectedCombined, RowUtil.combineRows(row1, row2, row1.length + row2.length));


    }
}
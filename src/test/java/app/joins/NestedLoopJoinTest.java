package app.joins;

import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class NestedLoopJoinTest {
    private final Path leftFile = Path.of("src/test/resources/email.csv");
    private final Path rightFile = Path.of("src/test/resources/username.csv");
    private final String columnName = "id";

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void innerJoin() throws IOException, CsvException {
        NestedLoopJoin innerJoin = new NestedLoopJoin(leftFile, rightFile, columnName, JoinType.INNER);
        innerJoin.join(true);

        String innerJoinExpectedResult = "\"email\",\"id\",\"name\",\"surname\",\"username\",\"id\",\"name\",\"surname\"\n" +
                                         "\"craig@example.com\",\"4081\",\"Craig\",\"Johnson\",\"johnson81\",\"4081\",\"Craig\",\"Johnson\"\n" +
                                         "\"mary@example.com\",\"9346\",\"Mary\",\"Jenkins\",\"jenkins46\",\"9346\",\"Mary\",\"Jenkins\"\n" +
                                         "\"jamie@example.com\",\"5079\",\"Jamie\",\"Smith\",\"smith79\",\"5079\",\"Jamie\",\"Smith\"\n" +
                                         "\"laura@example.com\",\"2070\",\"Laura\",\"Grey\",\"grey07\",\"2070\",\"Laura\",\"Grey\"\n";

        assertEquals(innerJoinExpectedResult, outContent.toString());
    }

    @Test
    void leftJoin() throws IOException, CsvException {
        NestedLoopJoin leftJoin = new NestedLoopJoin(leftFile, rightFile, columnName, JoinType.LEFT);
        leftJoin.join(true);

        String leftJoinExpectedResult = "\"email\",\"id\",\"name\",\"surname\",\"username\",\"id\",\"name\",\"surname\"\n" +
                                        "\"craig@example.com\",\"4081\",\"Craig\",\"Johnson\",\"johnson81\",\"4081\",\"Craig\",\"Johnson\"\n" +
                                        "\"mary@example.com\",\"9346\",\"Mary\",\"Jenkins\",\"jenkins46\",\"9346\",\"Mary\",\"Jenkins\"\n" +
                                        "\"jamie@example.com\",\"5079\",\"Jamie\",\"Smith\",\"smith79\",\"5079\",\"Jamie\",\"Smith\"\n" +
                                        "\"john@example.com\",\"1111\",\"John\",\"Smith\",,,,\n" +
                                        "\"laura@example.com\",\"2070\",\"Laura\",\"Grey\",\"grey07\",\"2070\",\"Laura\",\"Grey\"\n";

        assertEquals(leftJoinExpectedResult, outContent.toString());
    }

    @Test
    void rightJoin() throws IOException, CsvException {
        NestedLoopJoin rightJoin = new NestedLoopJoin(leftFile, rightFile, columnName, JoinType.RIGHT);
        rightJoin.join(true);

        String rightJoinExpectedResult = "\"email\",\"id\",\"name\",\"surname\",\"username\",\"id\",\"name\",\"surname\"\n" +
                                         "\"laura@example.com\",\"2070\",\"Laura\",\"Grey\",\"grey07\",\"2070\",\"Laura\",\"Grey\"\n" +
                                         "\"craig@example.com\",\"4081\",\"Craig\",\"Johnson\",\"johnson81\",\"4081\",\"Craig\",\"Johnson\"\n" +
                                         "\"mary@example.com\",\"9346\",\"Mary\",\"Jenkins\",\"jenkins46\",\"9346\",\"Mary\",\"Jenkins\"\n" +
                                         "\"jamie@example.com\",\"5079\",\"Jamie\",\"Smith\",\"smith79\",\"5079\",\"Jamie\",\"Smith\"\n" +
                                         ",,,,\"anne78\",\"2222\",\"Anne\",\"Grey\"\n" +
                                         ",,,,\"tom12\",\"\",\"Tom\",\"Booker\"\n" +
                                         ",,,,\"booker12\",\"9012\",\"\",\"Booker\"\n";

        assertEquals(rightJoinExpectedResult, outContent.toString());
    }
}
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

class GraceHashJoinTest {
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
        GraceHashJoin innerJoin = new GraceHashJoin(leftFile, rightFile, columnName, JoinType.INNER);
        innerJoin.join(true);

        String innerJoinExpectedResult = """
                "email","id","name","surname","username","id","name","surname"
                "laura@example.com","2070","Laura","Grey","grey07","2070","Laura","Grey"
                "mary@example.com","9346","Mary","Jenkins","jenkins46","9346","Mary","Jenkins"
                "jamie@example.com","5079","Jamie","Smith","smith79","5079","Jamie","Smith"
                "craig@example.com","4081","Craig","Johnson","johnson81","4081","Craig","Johnson"
                """;

        assertEquals(innerJoinExpectedResult, outContent.toString());
    }

    @Test
    void leftJoin() throws IOException, CsvException {
        GraceHashJoin leftJoin = new GraceHashJoin(leftFile, rightFile, columnName, JoinType.LEFT);
        leftJoin.join(true);

        String leftJoinExpectedResult = """
                "email","id","name","surname","username","id","name","surname"
                "grey07","2070","Laura","Grey","laura@example.com","2070","Laura","Grey"
                "jenkins46","9346","Mary","Jenkins","mary@example.com","9346","Mary","Jenkins"
                "smith79","5079","Jamie","Smith","jamie@example.com","5079","Jamie","Smith"
                "john@example.com","1111","John","Smith",,,,
                "johnson81","4081","Craig","Johnson","craig@example.com","4081","Craig","Johnson"
                """;

        assertEquals(leftJoinExpectedResult, outContent.toString());
    }

    @Test
    void rightJoin() throws IOException, CsvException {
        GraceHashJoin rightJoin = new GraceHashJoin(leftFile, rightFile, columnName, JoinType.RIGHT);
        rightJoin.join(true);

        String rightJoinExpectedResult = """
                "email","id","name","surname","username","id","name","surname"
                ,,,,"anne78","2222","Anne","Grey"
                ,,,,"tom12","","Tom","Booker"
                "laura@example.com","2070","Laura","Grey","grey07","2070","Laura","Grey"
                "mary@example.com","9346","Mary","Jenkins","jenkins46","9346","Mary","Jenkins"
                "jamie@example.com","5079","Jamie","Smith","smith79","5079","Jamie","Smith"
                "craig@example.com","4081","Craig","Johnson","johnson81","4081","Craig","Johnson"
                ,,,,"booker12","9012","","Booker"
                """;

        assertEquals(rightJoinExpectedResult, outContent.toString());
    }
}
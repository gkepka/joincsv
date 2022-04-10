package app.joins;

import app.utils.JoinType;
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
    void join() throws IOException, CsvException {
        Path leftFile = Path.of("src/test/resources/email.csv");
        Path rightFile = Path.of("src/test/resources/username.csv");

        String columnName = "id";

        GraceHashJoin innerJoin = new GraceHashJoin(leftFile, rightFile, columnName, JoinType.INNER);
        innerJoin.join(true);

        System.out.println(outContent.toString());

        fail();


    }
}
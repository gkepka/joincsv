package app.utils;

import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVUtilTest {

    private CSVUtil usernameReader;
    private CSVUtil tmpWriter;
    private Path tmpFile;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;


    @BeforeEach
    void setUp() throws IOException {
        System.setOut(new PrintStream(outContent));
        usernameReader = new CSVUtil(Path.of("src/test/resources/username.csv"), false);
        tmpFile = Files.createTempFile("test", ".csv");
        tmpWriter = new CSVUtil(tmpFile, true);

    }

    @AfterEach
    void tearDown() throws IOException {
        usernameReader.close();
        tmpWriter.close();
        Files.delete(tmpFile);
        System.setOut(originalOut);
    }

    @Test
    void readCSVFile() throws IOException, CsvException {
        List<String[]> rows = usernameReader.readCSVFile();
        assertArrayEquals(rows.get(0), new String[]{"username","id","name","surname"});
        assertArrayEquals(rows.get(5), new String[]{"smith79","5079","Jamie","Smith"});
        assertThrows(IllegalStateException.class, () -> tmpWriter.readCSVFile());
    }

    @Test
    void readRows() throws IOException, CsvException {
        List<String[]> rows = usernameReader.readRows(2);
        assertEquals(rows.size(), 2);
        assertArrayEquals(rows.get(0), new String[]{"username","id","name","surname"});
        assertArrayEquals(rows.get(1), new String[]{"booker12","9012","","Booker"});

        assertThrows(IllegalStateException.class, () -> tmpWriter.readRows(2));
    }

    @Test
    void readRow() throws IOException, CsvException {
        String[] row = usernameReader.readRow();
        assertArrayEquals(row, new String[]{"username","id","name","surname"});
        row = usernameReader.readRow();
        assertArrayEquals(row, new String[]{"booker12","9012","","Booker"});

        assertThrows(IllegalStateException.class, () -> tmpWriter.readRow());
    }

    @Test
    void writeRow() throws IOException, CsvException {
        String[] row = {"1", "2", "3"};
        tmpWriter.writeRow(row);
        tmpWriter.flush();

        CSVUtil tmpReader = new CSVUtil(tmpFile, false);
        String[] readRow = tmpReader.readRow();
        tmpReader.close();

        assertArrayEquals(row, readRow);

        assertThrows(IllegalStateException.class, () -> usernameReader.writeRow(row));
    }

    @Test
    void printRowToStdout() throws IOException {
        String[] row = {"1", "2", "3"};
        tmpWriter.printRowToStdout(row);
        tmpWriter.flush();
        assertEquals("\"1\",\"2\",\"3\"\n", outContent.toString());

        System.setOut(originalOut);
    }

}
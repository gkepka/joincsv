package app.utils;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CSVUtil implements AutoCloseable  {
    private final boolean write;
    private final CSVReader reader;
    private final CSVWriter writer;
    private final CSVWriter stdoutWriter = new CSVWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

    public CSVUtil(Path path, boolean write) throws IOException {
        this.write = write;
        if (write) {
            this.writer = new CSVWriter(Files.newBufferedWriter(path));
            this.reader = null;
        } else {
            ICSVParser parser = new RFC4180Parser();
            CSVReaderBuilder builder = new CSVReaderBuilder(Files.newBufferedReader(path)).withCSVParser(parser);
            this.reader = builder.build();
            this.writer = null;
        }
    }

    public List<String[]> readCSVFile() throws IOException, CsvException {
        if (write) {
            throw new IllegalStateException("Object set for writing to file");
        }
        return reader.readAll();
    }

    public List<String[]> readRows(int count) throws IOException, CsvException {
        if (write) {
            throw new IllegalStateException("Object set for writing to file");
        }
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String[] row = reader.readNext();
            if (row == null) {
                break;
            } else {
                rows.add(row);
            }
        }
        return rows;
    }

    public String[] readRow() throws IOException, CsvException {
        return readRows(1).get(0);
    }

    public void writeRow(String[] row) {
        if (!write) {
            throw new IllegalStateException("Object set for reading from file");
        }
        writer.writeNext(row);
    }

    public void printRowToStdout(String[] row) {
        stdoutWriter.writeNext(row);
    }

    public void flush() throws IOException {
        writer.flush();
        stdoutWriter.flush();
    }

    @Override
    public void close() throws IOException {
        if (write) {
            writer.close();
        } else {
            reader.close();
        }
        stdoutWriter.flush();
    }
}

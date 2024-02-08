package com.grigoriliev.csv.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public final class CsvUtil {
	private CsvUtil() {}

	/**
	 * The returned stream should be closed.
	 */
	public static Stream<CSVRecord> stream(String csvFilePath) throws IOException {
		return stream(Files.newBufferedReader(Paths.get(csvFilePath)));
	}

	/**
	 * The returned stream should be closed.
	 */
	public static Stream<CSVRecord> stream(InputStream inputStream) throws IOException {
		return stream(new InputStreamReader(inputStream));
	}

	/**
	 * The returned stream should be closed.
	 */
	public static Stream<CSVRecord> stream(Reader reader) throws IOException {
		final CSVParser csvParser = new CSVParser(
			reader,
			CSVFormat.Builder.create()
				.setHeader().setSkipHeaderRecord(true)
				.setTrim(true)
				.build()
		);

		return csvParser.stream().onClose(
			() -> {
				try {
					csvParser.close();
					reader.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		);
	}

	public static void write(
		Stream<String[]> rowStream, String csvFilePath, String... headerFields
	) throws IOException {
		try (
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(csvFilePath));
			CSVPrinter csvPrinter = new CSVPrinter(
				writer,
				CSVFormat.Builder.create()
					.setHeader(headerFields)
					.setTrim(true)
					.build()
			)
		) {
			rowStream.forEach(
				row -> {
					try {
						csvPrinter.printRecord(row);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			);
		}
	}
}

package com.man.demo.Services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;


import ch.qos.logback.classic.Logger;


@Service
public class LogFileProcessor {
	private static final Logger logger = (Logger) LoggerFactory.getLogger(LogFileProcessor.class);
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static Random random = new Random();
	public String processFilePath(String directoryPath,String desiredFileName)  throws IOException {
		
		long startTime = System.currentTimeMillis();

	
		int randomNumber = random.nextInt(100); 
		String folderName = desiredFileName +randomNumber+ "-" + "output";
		String filePath = directoryPath + File.separator + desiredFileName;
		File fileToProcess = new File(filePath);
		String outputDirectoryPath = directoryPath + File.separator + folderName;

		File outputDirectory = new File(outputDirectoryPath);
		createFolder(outputDirectory, outputDirectoryPath);
		if (fileToProcess.exists() && fileToProcess.isFile()) {
			Map<String, BufferedWriter> columnWriters = new ConcurrentHashMap<>();

			int batchSize = 1000;
			List<String> logEntryBatch = new ArrayList<>();

			try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
				processLogFile(reader, batchSize, logEntryBatch, columnWriters, outputDirectoryPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
   

			Runtime runtime = Runtime.getRuntime();
			long maxMemory = runtime.maxMemory();
			long totalMemory = runtime.totalMemory();
			long freeMemory = runtime.freeMemory();
			long usedMemory = totalMemory - freeMemory;
			cleanupWriters(columnWriters);
	       
			printMemoryInfo(maxMemory, totalMemory, freeMemory, usedMemory, startTime);
			logger.info("Process over new");
		}
		
  return outputDirectoryPath;
	}
	
	private static void createFolder(File outputDirectory, String outputDirectoryPath) {
		if (!outputDirectory.exists()) {
			if (outputDirectory.mkdirs()) {
				logger.info("Output directory created: {}", outputDirectoryPath);
			} else {
				logger.error("Failed to create output directory.");
			}
		}
	}
	private static void processLogFile(BufferedReader reader, int batchSize, List<String> logEntryBatch,
			Map<String, BufferedWriter> columnWriters, String outputDirectoryPath)
			throws IOException {
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				if (line.trim().startsWith("{")) {
					logEntryBatch.add(line);
					if (logEntryBatch.size() >= batchSize) {
						processAndWriteBatch(logEntryBatch, columnWriters, outputDirectoryPath);
						logEntryBatch.clear();
					}
				} else {
					logger.error("Invalid JSON data: {}", line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!logEntryBatch.isEmpty()) {
			processAndWriteBatch(logEntryBatch, columnWriters, outputDirectoryPath);
			logEntryBatch.clear();
		}
	}

	private static void cleanupWriters(Map<String, BufferedWriter> columnWriters) {
		for (BufferedWriter writer : columnWriters.values()) {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	private static void printMemoryInfo(long maxMemory, long totalMemory, long freeMemory, long usedMemory,
			long startTime) {
		logger.info("Max Memory (MB): {}", (maxMemory / (1024 * 1024)));
		logger.info("Total Memory (MB): {}", (totalMemory / (1024 * 1024)));
		logger.info("Free Memory (MB): {}", (freeMemory / (1024 * 1024)));
		logger.info("Used Memory (MB): {}", (usedMemory / (1024 * 1024)));

		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		logger.info("Time taken for file processing (milliseconds): {}", elapsedTime);

		double seconds = elapsedTime / 1000.0;
		logger.info("Time taken for file processing (seconds):{}", seconds);
	}

	// Method to process and write a batch of log entries
	private static void processAndWriteBatch(List<String> logEntries, Map<String, BufferedWriter> columnWriters,
			String outputDirectoryPath) {
		logEntries.parallelStream().forEach(logEntry -> processJsonObject(logEntry, columnWriters, outputDirectoryPath));
	}

	// Process each line of the file i.e, each json object
	private static void processJsonObject(String jsonString, Map<String, BufferedWriter> columnWriters,
			String outputDirectoryPath) {

		try {
			JsonFactory factory = new JsonFactory();
			JsonParser parser = factory.createParser(jsonString);
			processJsonNode(parser, columnWriters, outputDirectoryPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void processJsonNode(JsonParser parser, Map<String, BufferedWriter> columnWriters,
			String outputDirectoryPath) throws IOException {
		String currentKey = "";
		Deque<String> keyStack = new ArrayDeque<>();
		while (parser.nextToken() != null) {
			JsonToken token = parser.currentToken();
			if (token == JsonToken.START_OBJECT) {
				keyStack.push(currentKey);
				currentKey = "";
			} else if (token == JsonToken.END_OBJECT) {
				if(!keyStack.isEmpty())currentKey = keyStack.pop();
			} else if (token == JsonToken.FIELD_NAME) {

				currentKey = currentKey.isEmpty() && !keyStack.isEmpty() ? keyStack.pop() + "." + parser.getText()
						: parser.getText();
			} else if (token.isScalarValue()) {
				if (currentKey.equals(".timestamp"))
					currentKey = "timestamp";
				String columnName = currentKey + ".column";
				currentKey = "";
				 BufferedWriter writer = columnWriters.get(columnName);
	                if (writer == null) {
	                    writer = createColumnWriter(outputDirectoryPath, columnName);
	                    columnWriters.put(columnName, writer);
	                }
	                String valueToBeWritten=parser.getValueAsString() ;
	                if(valueToBeWritten !=null)writeValueToFile(writer,valueToBeWritten );
			}
		}

	}

	// for each new column file creates a new buffered writer
	private static BufferedWriter createColumnWriter(String outputDirectoryPath, String columnName) {

		try {
			File file = new File(outputDirectoryPath, columnName);
			return new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	// writes value to the column files by using buffer writer
	private static void writeValueToFile(BufferedWriter writer, String value) throws NullPointerException {

		try {
			writer.write(value);
			writer.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

package com.man.demo.Controllers;

import java.io.IOException;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.man.demo.Data.DataDto;
import com.man.demo.Services.LogFileProcessor;

@RestController
@RequestMapping("/processLogs")
public class FileProcessingController {

	private LogFileProcessor logFileProcessor;

	public FileProcessingController(LogFileProcessor logFileProcessor) {
		this.logFileProcessor = logFileProcessor;
	}

	@PostMapping()
	public DataDto processFilePath(@RequestBody DataDto dataDto) throws IOException {

		String directoryPath = dataDto.getDirectory();
		String desiredFileName = dataDto.getFileName();
		if (directoryPath.length()<3  || desiredFileName.length()<3) {
			dataDto.setProcessedFileUrl("Please Enter File Path and File name");
			return dataDto;
		}

		else {
			String outputDirectoryPath = logFileProcessor.processFilePath(directoryPath, desiredFileName);
			dataDto.setProcessedFileUrl(outputDirectoryPath);
			return dataDto;
		}

	}
}

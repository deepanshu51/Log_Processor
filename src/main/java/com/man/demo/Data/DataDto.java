package com.man.demo.Data;

//Data handling from frontend
public class DataDto {
    private String directory;
    private String fileName;
    private String processedFileUrl; 

    public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getProcessedFileUrl() {
        return processedFileUrl;
    }

    public void setProcessedFileUrl(String processedFileUrl) {
        this.processedFileUrl = processedFileUrl;
    }
}



package org.dueam.hadoop.common;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;

public class LineFile {
	public static interface Line {
		public void doLine(String line);
	}
	int mod = 0;
	public BufferedReader reader;


	public void read(Line lineCallback) throws IOException {
		if(mod != 1){
			System.out.println("You can only read!");
			return ;
		}
		String line = reader.readLine();
		while (line != null) {
			lineCallback.doLine(line);
			line = reader.readLine();
		}
	}

	public static LineFile getRead(String path) throws UnsupportedEncodingException, FileNotFoundException{
		InputStreamReader reader0 = new InputStreamReader(new FileInputStream(path), "GBK");
		BufferedReader reader = new BufferedReader(reader0);
		LineFile line = new LineFile();
		line.reader = reader;
		line.mod = 1;
		return line;
	}
	public static LineFile getWrite(String path) throws UnsupportedEncodingException, FileNotFoundException{
		LineFile line = new LineFile();
		line.output = new FileOutputStream(path);
		line.mod = 2;
		return line;
	}

	public void close() throws IOException{
		if(reader != null){
			reader.close();
		}
		if(output != null){
			output.close();
		}
	}

	public OutputStream output = null;
	public void write(String line) throws IOException {
		if(mod != 2){
			System.out.println("You can only write!");
			return ;
		}
		IOUtils.write(line, output,"GBK");

	}

}

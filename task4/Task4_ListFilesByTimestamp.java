package hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task4_ListFilesByTimestamp {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length == 0) {
			System.out.println("Pass at least one argument");
			System.exit(1);
		}
		long startTime = 0;
		long endTime = Long.MAX_VALUE;
		
		if(args.length == 2) {
			startTime = Long.parseLong(args[1]);
		} else if(args.length == 3) {
			startTime = Long.parseLong(args[1]);
			endTime = Long.parseLong(args[2]);
		}
		String inputPath = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		listFiles(inputPath, fs, true, startTime, endTime);
		fs.close();
	}
	
	public static void listFiles(String hdfsPath, FileSystem fs, boolean recursive, long startTime, long endTime) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
		if(fileStatuses!=null && fileStatuses.length > 0) {
			for(FileStatus fileStatus: fileStatuses) {
				long modificationTime = fileStatus.getModificationTime();
				if (modificationTime>=startTime && modificationTime<=endTime) {
					System.out.println((fileStatus.isDirectory()?"Directory":"File")+"\t"+fileStatus.getPath().toString());
				}
				
				if(fileStatus.isDirectory() && recursive) {
					listFiles(fileStatus.getPath().toString(),fs,recursive, startTime, endTime);
				}
			}
		}
	} 

}

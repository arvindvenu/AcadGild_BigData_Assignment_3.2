package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Task5_DisplayFileContent {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length == 0) {
			System.out.println("Pass one argument");
			System.exit(1);
		}
		
		String inputPath = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path filePath = new Path(inputPath);
		if(fs.isFile(filePath)) {
			FSDataInputStream inputDataStream = fs.open(filePath);
			IOUtils.copyBytes(inputDataStream, System.out, 4096, false);
			IOUtils.closeStream(inputDataStream);
			fs.close();
		} else {
			System.out.println("Please sepcify path to a file");
			fs.close();
			System.exit(1);
		}
		
	}

}

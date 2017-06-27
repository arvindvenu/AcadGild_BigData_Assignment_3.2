package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task6_CopyLocalToHDFS {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.out.println("Pass two arguments");
			System.exit(1);
		}
		String sourceLocalPath = args[0];
		String destHdfsPath = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path(sourceLocalPath), new Path(destHdfsPath));
		fs.close();
	}

}

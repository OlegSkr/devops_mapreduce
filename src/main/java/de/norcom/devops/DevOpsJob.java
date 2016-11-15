package de.norcom.devops;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DevOpsJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		createFileOnHDFS();

		String inputTableName = "devops";
		String outputTableName = "devops2";

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		Job job = Job.getInstance(getConf(), "DevOpsJob");
		job.setJarByClass(DevOpsJob.class);

		TableMapReduceUtil.initTableMapperJob(inputTableName, scan, DevOpsMapper.class,
			ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(outputTableName, DevOpsReducer.class, job);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	private void createFileOnHDFS() throws IOException {

		byte[] buf = getBuffer();

		Configuration configuration = getConf();
		// configuration = HBaseConfiguration.create(configuration);

		FileSystem fs = FileSystem.get(configuration);

		String dirName = "/devops";

		Path dirPath = new Path(dirName);

		Path filePath = new Path(dirName + "/import.txt");

		boolean res = fs.mkdirs(dirPath);
		if (!res) {
			String message = "mkdir failed, path: " + dirPath;
			System.out.println(message);
			throw new IOException(message);
		}

		System.out.println("mkdir( " + dirPath + ") went ok, now writing file");

		{
			// create wfile
			FSDataOutputStream ostr = fs.create(filePath, true, // overwrite
				512, // buffersize
				(short) 1, // replication
				64 * 1024 * 1024 // chunksize
			);
			ostr.write(buf);
			ostr.close();
		}

		System.out.println("write( " + filePath + ") went ok");
	}

	private byte[] getBuffer() throws IOException {
		InputStream is = getClass().getResourceAsStream("/import.txt");

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] read = new byte[512]; // Your buffer size.
		for (int i; -1 != (i = is.read(read)); os.write(read, 0, i))
			;
		is.close();

		return os.toByteArray();
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DevOpsJob(), args);
		System.exit(exitCode);
	}

}

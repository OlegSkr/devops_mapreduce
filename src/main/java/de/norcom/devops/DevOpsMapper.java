package de.norcom.devops;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class DevOpsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {

		String keyString = Bytes.toString(key.get());
		String valueString = Bytes.toString(value.getValue(Bytes.toBytes("devops"), Bytes.toBytes("body")));

		System.out.println(getClass().getSimpleName() + ": " + keyString + ": " + valueString);

		context.write(key, new ImmutableBytesWritable(Bytes.toBytes(valueString)));

	}

}

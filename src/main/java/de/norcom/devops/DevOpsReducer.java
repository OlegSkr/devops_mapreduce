package de.norcom.devops;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;

public class DevOpsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, Object> {

	private static final String INDEX_NAME = "index";

	private JestClient client;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		JestClientFactory factory = new JestClientFactory();

		Configuration configuration = context.getConfiguration();

		String hostname = "10.0.2.1";

		String address = configuration.get("norcom.devops.elastic.address");

		System.out.println("DevOpsReducer.setup(\"norcom.devops.elastic.address\") = " + address);

		if (address != null && !address.isEmpty()) {
			hostname = address;
		}

		factory.setHttpClientConfig(
			new HttpClientConfig.Builder("http://" + hostname + ":9200").multiThreaded(true).build());
		client = factory.getObject();

		boolean indexExists = client.execute(new IndicesExists.Builder(INDEX_NAME).build()).isSucceeded();
		if (indexExists) {
			client.execute(new DeleteIndex.Builder(INDEX_NAME).build());
		}
		client.execute(new CreateIndex.Builder(INDEX_NAME).build());

	}

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values,
			Context context) throws IOException, InterruptedException {

		Builder bulkIndexBuilder = new Builder();

		for (ImmutableBytesWritable value : values) {

			String keyString = Bytes.toString(key.get());
			String valueString = Bytes.toString(value.get());

			System.out.println(getClass().getSimpleName() + ": " + keyString + ": " + valueString);

			Put put = new Put(key.get());

			put.addColumn(Bytes.toBytes("devops"), Bytes.toBytes("body2"), value.get());

			context.write(null, put);

			Map<String, Object> article = new HashMap<String, Object>();
			article.put("body", valueString);

			bulkIndexBuilder.addAction(
				new Index.Builder(article).index(INDEX_NAME).id(keyString).type("article").build());

		}

		client.execute(bulkIndexBuilder.build());
	}

}

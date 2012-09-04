package cascalog.cartodb;

import com.cartodb.CartoDBClientIF;
import com.cartodb.CartoDBException;
import com.cartodb.impl.ApiKeyCartoDBClient;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * InputFormat for CartoDB MapReduce job.
 */
public class CartoDBInputFormat implements JobConfigurable, InputFormat<Text,
        Text> {
    private static final Logger LOG = Logger.getLogger(CartoDBInputFormat.class);
    private CartoDBClientIF client;

    /**
     * Configure MapReduce job by creating CartoDB client with account and
     * API Key supplied by the JobConf.
     *
     * @param job The JobConf
     */
    @Override
    public void configure(JobConf job) {
        try {
            client = new ApiKeyCartoDBClient(job.get("account"),
                    job.get("apiKey"));
        } catch (CartoDBException e) {
            throw new RuntimeException("Unable to create CartoDB client",
                    e.getCause());
        }
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        try {
            return new CartoDBRecordReader((DBInputSplit) split, job);
        } catch (CartoDBException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int splitCount) throws
            IOException {
        int count = 0;
        try {
            String sql = job.get("sqlCount");
            String response = query(job);//client.executeQuery(job.get
            // ("sqlCount"));
            count = Integer.parseInt(response.split("\n")[1]);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        String sql = job.get("sql");
        int splitSize = count / splitCount;
        InputSplit[] splits = new InputSplit[splitSize];
        for (int i = 0; i < splitCount; i++) {
            splits[i] = new DBInputSplit(sql, splitSize, i * splitSize);
        }
        return splits;
    }

    private String query(JobConf job) throws Exception {
        String sqlQuery = URLEncoder.encode(job.get("sqlCount"), "UTF-8");
        String params = "q=" + sqlQuery + "&api_key=" + job.get("apiKey")
                + "&format=csv";
        return IOUtils.toString(new URL("https://vertnet.cartodb" +
                ".com/api/v1/sql" + "?" +
                params), "UTF-8");
    }

    static class DBInputSplit implements InputSplit {
        String sql;
        long limit;
        long offset;

        public DBInputSplit() {
        }

        DBInputSplit(String sql, long limit, long offset) {
            this.sql = sql;
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public long getLength() throws IOException {
            return limit;
        }

        @Override
        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sql = Text.readString(in);
            offset = in.readLong();
            limit = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, sql);
            out.writeLong(offset);
            out.writeLong(limit);
        }
    }

    protected class CartoDBRecordReader implements RecordReader<Text,
            Text> {
        private JobConf job;
        private DBInputSplit split;
        private UnmodifiableIterator<String> records;
        private long pos;

        CartoDBRecordReader(DBInputSplit split, JobConf job) throws
                CartoDBException {
            this.split = split;
            this.job = job;
            String response = client.executeQuery(getSql(split));
            records = ImmutableList.copyOf(Splitter.on("\n").split(response))
                    .iterator();
        }

        private String getSql(DBInputSplit split) {
            return Joiner.on(" ").join(split.sql, "LIMIT", split.limit,
                    "OFFSET", split.offset);
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void close() throws IOException {
            // NOP
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public Text createValue() {
            return ReflectionUtils.newInstance(Text.class, job);
        }

        @Override
        public float getProgress() throws IOException {
            return pos / (float) split.limit;
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (!records.hasNext()) {
                return false;
            }
            String record = records.next();
            key.set(Splitter.on(",").split(record).iterator().next());
            value.set(record);
            return true;
        }
    }
}
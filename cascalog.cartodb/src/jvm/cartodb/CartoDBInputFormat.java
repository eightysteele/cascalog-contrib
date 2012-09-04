package cascalog.cartodb;

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

    /**
     * Configure MapReduce job by creating CartoDB client with account and
     * API Key supplied by the JobConf.
     *
     * @param job The JobConf
     */
    @Override
    public void configure(JobConf job) {
        // NOP
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        return new CartoDBRecordReader((CartoDBInputSplit) split, job);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int splitCount) throws
            IOException {
        int count = 0;
        try {
            String sql = job.get("sqlCount");
            String response = query(job);
            count = Integer.parseInt(response.split("\n")[1]);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        String sql = job.get("sql");
        int splitSize = count / splitCount;
        InputSplit[] splits = new InputSplit[splitSize];
        for (int i = 0; i < splitCount; i++) {
            splits[i] = new CartoDBInputSplit(sql, splitSize, i * splitSize);
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

    static class CartoDBInputSplit implements InputSplit {
        String sql;
        long limit;
        long offset;

        public CartoDBInputSplit() {
        }

        CartoDBInputSplit(String sql, long limit, long offset) {
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
        private CartoDBInputSplit split;
        private UnmodifiableIterator<String> records;
        private long pos;

        CartoDBRecordReader(CartoDBInputSplit split, JobConf job) throws IOException {
            this.split = split;
            this.job = job;
            String sqlQuery = URLEncoder.encode(job.get("sql"), "UTF-8");
            String params = "q=" + sqlQuery + "&api_key=" + job.get("apiKey")
                    + "&format=csv";
            String response = IOUtils.toString(new URL("https://vertnet" +
                    ".cartodb" + ".com/api/v1/sql" + "?" + params), "UTF-8");
            records = ImmutableList.copyOf(Splitter.on("\n").split(response))
                    .iterator();
        }

        private String getSql(CartoDBInputSplit split) {
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
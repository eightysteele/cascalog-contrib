package cascalog.cartodb;

import cascading.flow.FlowProcess;
import cascading.tap.SourceTap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryIterator;
import com.cartodb.CartoDBException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Represents an authenticated CartoDB resource.
 */
public class CartoDBTap extends SourceTap<JobConf, RecordReader> {
    private static final Logger LOGGER = Logger.getLogger(CartoDBTap.class);
    private final String account;
    private final String apiKey;
    private final String sql;
    private final String sqlCount;

    public static final CartoDBTap create(CartoDBScheme scheme, String account,
            String apiKey, String sql, String sqlCount) throws
            CartoDBException {
        Preconditions.checkNotNull(account, "account is required.");
        Preconditions.checkNotNull(apiKey, "apiKey is required.");
        Preconditions.checkNotNull(sql, "sql is required.");
        Preconditions.checkNotNull(sqlCount, "sqlCount is required.");
        return new CartoDBTap(scheme, account, apiKey, sql, sqlCount);
    }

    private CartoDBTap(CartoDBScheme scheme, String account, String apiKey,
            String sql, String sqlCount) {
        super(scheme);
        this.account = account;
        this.apiKey = apiKey;
        this.sql = sql;
        this.sqlCount = sqlCount;
    }

    @Override
    public String getIdentifier() {
        return sql; // TODO: Return CartoDB request URL.
    }

    @Override
    public long getModifiedTime(JobConf entries) throws IOException {
        return System.currentTimeMillis();  // TODO: This make sense?
    }

    @Override
    public TupleEntryIterator openForRead(
            FlowProcess<JobConf> jobConfFlowProcess,
            RecordReader recordReader) throws IOException {
        return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this,
                recordReader);
    }

    @Override
    public boolean resourceExists(JobConf entries) throws IOException {
        return true; // TODO: Send table exits SQL request.
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
        FileInputFormat.setInputPaths(conf, "http://" + account + ".cartodb" +
                ".com");
        conf.set("account", account);
        conf.set("apiKey", apiKey);
        conf.set("sql", sql);
        conf.set("sqlCount", sqlCount);
        super.sourceConfInit(process, conf);
    }
}
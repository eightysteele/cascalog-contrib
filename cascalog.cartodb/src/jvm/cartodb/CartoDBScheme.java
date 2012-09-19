package cascalog.cartodb;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.google.common.base.Splitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Scheme for converting between CartoDB JSON results and Tuple objects.
 */
public class CartoDBScheme
        extends Scheme<JobConf, RecordReader, OutputCollector,
        Object[], Object[]> {
    private static final Logger LOGGER = Logger.getLogger(CartoDBScheme.class);

    @Override
    public void sink(FlowProcess<JobConf> flowProcess,
            SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        // NOP
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf,
            RecordReader, OutputCollector> tap, JobConf conf) {
        // NOP
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) throws
            IOException {
        Object key = sourceCall.getContext()[0];
        Object value = sourceCall.getContext()[1];
        boolean result = sourceCall.getInput().next(key, value);
        if (!result) {
            return false;
        }
        Tuple t = new Tuple();
        for (String x : Splitter.on(",").split(value.toString())) {
            t.add(x);
        }
        sourceCall.getIncomingEntry().setTuple(t);
        return true;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf,
            RecordReader, OutputCollector> tap, JobConf job) {
        job.setInputFormat(CartoDBInputFormat.class);
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        Object[] pair = new Object[]{
                sourceCall.getInput().createKey(),
                sourceCall.getInput().createValue()
        };
        sourceCall.setContext(pair);
    }
}

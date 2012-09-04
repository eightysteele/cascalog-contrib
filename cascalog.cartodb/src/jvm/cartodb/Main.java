package cascalog.cartodb;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.cartodb.CartoDBException;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws CartoDBException {
        CartoDBScheme scheme = new CartoDBScheme();
        CartoDBTap tap = CartoDBTap.create(scheme, "vertnet",
                "b3263ddb2c8afddfa4bddca43d44727f9d3220ce",
                "select * from publishers",
                "select count(*) as count from publishers");

        //String inPath = args[ 0 ];
        String outPath = args[0];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        // create the source tap
        //Tap inTap = new Hfs( new TextDelimited( true, "\t" ), inPath );

        // create the sink tap
        Tap outTap = new Hfs(new TextDelimited(true, "\t"), outPath);

        // specify a pipe to connect the taps
        Pipe copyPipe = new Pipe("copy");

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(copyPipe, tap)
                .addTailSink(copyPipe, outTap);

        // run the flow
        Flow flow = flowConnector.connect(flowDef);

        flow.writeDOT("/home/eighty/cdb.dot");
        //flow.complete();
    }
}

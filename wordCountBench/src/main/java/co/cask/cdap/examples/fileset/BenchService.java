package co.cask.cdap.examples.fileset;


import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by peilunzh on 5/11/2015.
 */
public  class BenchService extends AbstractService{
    public static final String SERVICE_NAME = "Bench";
    static final byte[] ONE = {'1'};
    static final byte[] TWO = {'2'};
    static final byte[] THREE = {'3'};


    @Override
    protected void configure(){
        setName(SERVICE_NAME);
        setDescription("this returns the benchmark result");
        addHandler(new BenchHandler());
    }

    public  class BenchHandler extends AbstractHttpServiceHandler {

        @UseDataSet("benchData")
        private Table benchData;

        @Path("benchmark")
        @GET
        public void benchmark(HttpServiceRequest request, HttpServiceResponder responder) {
            Row result = benchData.get(new Get(ONE,ONE,TWO,THREE));
            if(result.isEmpty()){
                responder.sendError(404,"no result, should run wordcount first");
            }

            else {
                double startTime = result.getDouble(ONE);
                double endTime = result.getDouble(TWO);
                double benchSize = Double.valueOf(result.getLong(THREE).toString());


                double duration = (endTime - startTime)/1000; //from ms to s
                double throughput = benchSize / duration;

                Map<String, Object> results = new HashMap<String, Object>();

                results.put("Type", "wordcount");
                results.put("Duration,s", duration);
                results.put("BenchSize,Bytes", benchSize);
                results.put("Throughput,Bytes/s", throughput);


                responder.sendJson(200, results);
            }
        }
    }
}

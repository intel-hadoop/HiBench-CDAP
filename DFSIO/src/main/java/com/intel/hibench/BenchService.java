/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.intel.hibench;


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
public class BenchService extends AbstractService {
    public static final String SERVICE_NAME = "Bench";
    static final byte[] ONE = {'1'};
    static final byte[] TWO = {'2'};
    static final byte[] THREE = {'3'};


    @Override
    protected void configure() {
        setName(SERVICE_NAME);
        setDescription("this returns the benchmark result");
        addHandler(new BenchHandler());
    }

    public class BenchHandler extends AbstractHttpServiceHandler {

        @UseDataSet("benchData")
        private Table benchData;

        @Path("benchmark")
        @GET
        public void benchmark(HttpServiceRequest request, HttpServiceResponder responder) {
            Row result = benchData.get(new Get(ONE, ONE, TWO, THREE));
            if (result.isEmpty()) {
                responder.sendError(404, "no result, should run wordcount first");
            } else {
                double startTime = result.getDouble(ONE);
                double endTime = result.getDouble(TWO);
                double benchSize = Double.valueOf(result.getLong(THREE).toString());


                double duration = (endTime - startTime) / 1000; //from ms to s
                long throughput = (long) (benchSize / duration);

                Map<String, Object> results = new HashMap<String, Object>();

                results.put("processMethod", "Hadoop MR");
                results.put("benchduration", duration);
                results.put("benchsize", benchSize);
                results.put("throughput", throughput);


                responder.sendJson(200, results);
            }
        }
    }
}

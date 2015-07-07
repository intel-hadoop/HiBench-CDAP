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

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.nio.ByteBuffer;

/**
 * Created by peilunzh on 5/27/2015.
 * This service offers you a web UI to run your DFSIO benchmark
 */
public class BenchUI extends AbstractService {
    public static final String SERVICE_NAME = "BenchUI";

    @Override
    protected void configure() {
        setName(SERVICE_NAME);
        setDescription("Service that creates a Web UI.");
        addHandler(new BenchUIHandler());
    }


    public static final class BenchUIHandler extends AbstractHttpServiceHandler {
        @Path("UI")
        @GET
        public void UI(HttpServiceRequest request, HttpServiceResponder responder) {

              /*InputStream is = this.getClass().getClassLoader().getResourceAsStream("index.html");
              byte[] bytes = new byte[4096];
              try {
                is.read(bytes);
              } catch (IOException e) {
                e.printStackTrace();
              }*/

            //minify the intex.html and put into the string
            String index = "<!DOCTYPE html><html><head><title>CDAP-Bench</title><script type=text/javascript src=http://code.jquery.com/jquery-2.1.4.min.js></script><style>body{text-align:center;font-family:Verdana,\"Microsoft Yahei\",serif;font-weight:700}.execute,fieldset{margin:0 auto;width:16.6%;text-align:left}fieldset{width:42%}button{margin-left:60%;margin-top:3%;margin-bottom:20px}input,select{width:100%;margin-top:2%;margin-bottom:3%}#execimg{float:right;display:none}#logo{margin-top:15px;width:150px;heigt:99px}table{width:100%;text-align:right}#tableTitle{text-align:center}</style><body><img id=logo src=http://www-scf.usc.edu/~smileham/itp104/images/intel-logo.png><h1>DFSIO</h1><p>DFSIO Benchmark for CDAP</p><br><div class=execute><label for=size>Input Filesize:</label><br><input id=size placeholder=\"Enter filesize(MB)\"><br><label for=method>Data Processing Method:</label><br><select id=method><option value=mr>Hadoop MR<option value=spark>Spark</select><br><label for=iterate>Iteration Times:</label><br><input id=iterate placeholder=\"Enter Iteration Times\"><br><button id=DFSIO class=program>Execute</button> <img id=execimg height=12% width=12% src=http://www.dabaoku.com/dabaoku/uploads/allimg/091021/2222023L0-8.gif></div><fieldset><legend>RESULT:</legend><table border=1 cellspacing=.5><tr id=tableTitle><td>Processing Method<td>Bench Size(Bytes)<td>Duration(s)<td>Throughput(Bytes/s)</table></fieldset><script>function dataGen(){iterateTimes=$(\"#iterate\").val(),$(\"#execimg\").show(),iterateGen=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/DFSIO/mapreduce/DFSIOWriter/status\",function(t){\"STOPPED\"==t.status&&0==lock&&(iterateTimes>0?(iterateTimes-=1,dataGenOnce()):(clearInterval(iterateGen),$(\"#execimg\").hide()))})},5e3)}function dataGenOnce(){lock=1;var t=$(\"#size\").val(),e={\"dataset.lines.output.path\":fileNameGen(),size:t},a=JSON.stringify(e);console.log(a),$.post(\"http://localhost:10000/v3/namespaces/default/apps/DFSIO/mapreduce/DFSIOWriter/start\",a,function(){console.log(\"Data: \"+a)}),setTimeout(resultGet(),2e3)}function resultGet(){var t=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/DFSIO/mapreduce/DFSIOWriter/status\",function(e){\"STOPPED\"===e.status&&$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/DFSIO/services/Bench/methods/benchmark\",function(e){console.log(e),$(\"#tableTitle\").after(\"<tr><td>\"+e.processMethod+\"</td><td>\"+e.benchsize+\"</td><td>\"+e.benchduration+\"</td><td>\"+e.throughput+\"</td></tr>\"),console.log(iterateTimes),lock=0,clearInterval(t)})})},400)}function fileNameGen(){for(var t=\"\",e=\"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\",a=0;6>a;a++)t+=e.charAt(Math.floor(Math.random()*e.length));return t}$(document).ready(function(){$(\"#DFSIO\").click(function(){dataGen()})});var iterateTimes,iterateGen,lock=0;</script>";
            byte[] indexByte = index.getBytes();

            responder.send(200, ByteBuffer.wrap(indexByte), "text/html", null);
        }
    }
}

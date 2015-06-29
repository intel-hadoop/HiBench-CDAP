package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.nio.ByteBuffer;

/**
 * Created by peilunzh on 5/27/2015.
 * This serves the UI to view the result
 */
public class BenchUI extends AbstractService{
    public static final String SERVICE_NAME="BenchUI";

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


            String index = "<!DOCTYPE html><html><head><title>CDAP-Bench-TeraSort</title><script type=text/javascript src=http://code.jquery.com/jquery-2.1.4.min.js></script><style>body{text-align:center;font-family:Verdana,\"Microsoft Yahei\",serif}.execute,.generate,fieldset{margin:0 auto;width:16.6%;text-align:left}fieldset{width:42%}button{margin-left:60%;margin-top:3%;margin-bottom:20px}input,select{width:100%;margin-top:2%;margin-bottom:3%}img{float:right;display:none}table{width:100%;text-align:right}#tableTitle{text-align:center}</style><body><h1>TeraSort</h1><p>TeraSort Benchmark for CDAP</p><br><div class=generate><label for=inputPath>Input Path:</label><br><input id=inputPath placeholder=\"Enter Input Path\"><br><label for=rows>Num of Rows:</label><br><input id=rows placeholder=\"enter number of rows(1 row = 100 bytes)\"><br><button id=TeraGen class=program>Generate</button> <img id=geneimg height=12% width=12% src=http://www.dabaoku.com/dabaoku/uploads/allimg/091021/2222023L0-8.gif></div><div class=execute><label for=method>Data Processing Method:</label><br><select id=method><option value=mr>Hadoop MR<option value=spark>Spark</select><br><label for=outputPath>Output Path:</label><br><input id=outputPath placeholder=\"Enter Output Path\"><br><button id=TeraSort class=program disabled>Execute</button> <img id=execimg height=12% width=12% src=http://www.dabaoku.com/dabaoku/uploads/allimg/091021/2222023L0-8.gif></div><fieldset><legend>RESULT:</legend><table border=1 cellspacing=.5><tr id=tableTitle><td>Processing Method<td>Bench Size(Bytes)<td>Duration(s)<td>Throughput(Bytes/s)</table></fieldset><script>$(document).ready(function(){var t,e,a;$(\"#TeraGen\").click(function(){t=$(\"#inputPath\").val(),a=$(\"#rows\").val();var e={\"dataset.lines.output.path\":t,rows:a},o=JSON.stringify(e);console.log(o),$.post(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/mapreduce/TeraGen/start\",o,function(){console.log(\"Data: \"+o)}),$(\"#geneimg\").show(),window.setTimeout(generateFinished(),3e3)}),$(\"#TeraSort\").click(function(){e=$(\"#outputPath\").val();var a={\"dataset.lines.input.paths\":t,\"dataset.counts.output.path\":e},o=JSON.stringify(a);console.log(o),$.post(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/mapreduce/TeraSort/start\",o,function(){console.log(\"Data: \"+o)}),$.post(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/services/Bench/start\",function(){console.log(\"start bench service\")}),$(\"#execimg\").show(),window.setTimeout(resultGet(),3e3)})});</script><script>function generateFinished(){var t=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/mapreduce/TeraGen/status\",function(e){\"STOPPED\"===e.status&&(console.log(e.status),$(\"#TeraSort\").removeAttr(\"disabled\"),$(\"#geneimg\").hide(),clearInterval(t),alert(\"Complete\"))})},500)}function resultGet(){var t=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/mapreduce/TeraSort/status\",function(e){\"STOPPED\"===e.status&&(console.log(e.status),$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/TeraSort/services/Bench/methods/benchmark\",function(e){console.log(e),$(\"#tableTitle\").after(\"<tr><td>\"+e.processMethod+\"</td><td>\"+e.benchsize+\"</td><td>\"+e.benchduration+\"</td><td>\"+e.throughput+\"</td></tr>\"),clearInterval(t),$(\"#execimg\").hide()}))})},500)}</script>";
            byte[] indexByte = index.getBytes();

            responder.send(200, ByteBuffer.wrap(indexByte), "text/html",null);
        }
    }
}

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


            String index = "<!DOCTYPE html><html><head><title>CDAP-Bench</title><script type=text/javascript src=http://code.jquery.com/jquery-2.1.4.min.js></script><style>body{text-align:center;font-family:Verdana,\"Microsoft Yahei\",serif;font-weight:700}.execute,.generate,fieldset{margin:0 auto;width:16.6%;text-align:left}fieldset{width:42%}button{margin-left:60%;margin-top:3%;margin-bottom:20px}input,select{width:100%;margin-top:2%;margin-bottom:3%}#execimg,#geneimg{float:right;display:none}#logo{margin-top:15px;width:150px;heigt:99px}table{width:100%;text-align:right}#tableTitle{text-align:center}</style><body><img id=logo src=http://www-scf.usc.edu/~smileham/itp104/images/intel-logo.png><h1>Word Count</h1><p>Word Count Benchmark for CDAP</p><br><div class=generate><label for=size>Input Filesize:</label><br><input id=size placeholder=\"enter filesize(MB)\"><br><button id=randomtextwriter class=program>Generate</button> <img id=geneimg height=12% width=12% src=http://www.dabaoku.com/dabaoku/uploads/allimg/091021/2222023L0-8.gif></div><div class=execute><label for=method>Data Processing Method:</label><br><select id=method><option value=mr>Hadoop MR<option value=spark>Spark</select><br><label for=iterate>Iteration Times:</label><br><input id=iterate placeholder=\"Enter Iteration Times\"><br><button id=wordcount class=program disabled>Execute</button> <img id=execimg height=12% width=12% src=http://www.dabaoku.com/dabaoku/uploads/allimg/091021/2222023L0-8.gif></div><fieldset><legend>RESULT:</legend><table border=1 cellspacing=.5><tr id=tableTitle><td>Processing Method<td>Bench Size(Bytes)<td>Duration(s)<td>Throughput(Bytes/s)</table></fieldset><script>function dataGen(){var t=$(\"#size\").val();inputPath=fileNameGen();var e={\"dataset.lines.output.path\":inputPath,size:t},a=JSON.stringify(e);console.log(a),$.post(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/mapreduce/RandomTextWriter/start\",a,function(){console.log(\"Data: \"+a)}),$(\"#geneimg\").show(),window.setTimeout(generateFinished(),3e3)}function generateFinished(){var t=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/mapreduce/RandomTextWriter/status\",function(e){\"STOPPED\"===e.status&&(console.log(e.status),$(\"#wordcount\").removeAttr(\"disabled\"),$(\"#geneimg\").hide(),clearInterval(t),alert(\"Complete\"))})},500)}function wordCount(){iterateTimes=$(\"#iterate\").val(),$(\"#execimg\").show(),iterateRun=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/DFSIO/mapreduce/DFSIOWriter/status\",function(t){\"STOPPED\"===t.status&&(iterateTimes>0?(iterateTimes-=1,wordCountOnce()):(clearInterval(iterateGen),$(\"#execimg\").hide()))})},5e3)}function wordCountOnce(){outputPath=fileNameGen();var t={\"dataset.lines.input.paths\":inputPath,\"dataset.counts.output.path\":outputPath},e=JSON.stringify(t);console.log(e),$.post(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/mapreduce/WordCount/start\",e,function(){console.log(\"Data: \"+e)}),$.post(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/services/Bench/start\",function(){console.log(\"start bench service\")}),$(\"#execimg\").show(),window.setTimeout(resultGet(),3e3)}function resultGet(){var t=setInterval(function(){$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/mapreduce/WordCount/status\",function(e){\"STOPPED\"===e.status&&(console.log(e.status),$.getJSON(\"http://localhost:10000/v3/namespaces/default/apps/WordCountBench/services/Bench/methods/benchmark\",function(e){console.log(e),$(\"#tableTitle\").after(\"<tr><td>\"+e.processMethod+\"</td><td>\"+e.benchsize+\"</td><td>\"+e.benchduration+\"</td><td>\"+e.throughput+\"</td></tr>\"),clearInterval(t)}))})},500)}function fileNameGen(){for(var t=\"\",e=\"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\",a=0;6>a;a++)t+=e.charAt(Math.floor(Math.random()*e.length));return t}$(document).ready(function(){$(\"#randomtextwriter\").click(function(){dataGen()}),$(\"#wordcount\").click(function(){wordCount()})});var inputPath,outputPath,iterateTimes,iterateRun;</script>";
            byte[] indexByte = index.getBytes();

            responder.send(200, ByteBuffer.wrap(indexByte), "text/html",null);
        }
    }
}

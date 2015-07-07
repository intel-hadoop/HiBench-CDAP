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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by peilunzh on 5/15/2015.
 * this write the file system with mapper
 */
public class DFSIOWriter extends AbstractMapReduce {

    private static final Logger LOG = LoggerFactory.getLogger(DFSIOWriter.class);

    @UseDataSet("benchData")
    private Table benchData;
    private static long numBytesToWrite = 100 * 1024 * 1024;
    private static String BENCH_SIZE = "size";
    public static double startTime = 2;
    public static double endTime = 1;

    static final byte[] ONE = {'1'};
    static final byte[] TWO = {'2'};
    static final byte[] THREE = {'3'};

    @Override
    public void configure() {
        setOutputDataset("lines");
        setMapperResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
        startTime = System.currentTimeMillis();
        benchData.put(new Put(ONE, ONE, startTime));

        Job job = context.getHadoopJob();
        job.setInputFormatClass(RandomInputFormat.class);
        job.setMapperClass(Generator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);


        String sizeStr = context.getRuntimeArguments().get("size");
        if (sizeStr != null) {
            LOG.info("size we get in config is : " + sizeStr);
            long totalBytes = Long.valueOf(sizeStr) * 1024 * 1024;
            job.getConfiguration().setLong(BENCH_SIZE, totalBytes);
            benchData.put(new Put(ONE, THREE, totalBytes));
        }

    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
        endTime = System.currentTimeMillis();
        benchData.put(new Put(ONE, TWO, endTime));

    }


    public static class Generator extends Mapper<Text, Text, Text, Text> {


        private Random random = new Random();
        Text aSentence = generateSentence(10);
        int bufferSize = aSentence.getLength();

        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {

            long totalBytes = context.getConfiguration().getLong(BENCH_SIZE, numBytesToWrite);
            LOG.info("Bytes we get in map is: " + String.valueOf(totalBytes));

            while (totalBytes > 0) {

                // Write the sentence
                context.write(aSentence, aSentence);

                totalBytes -= bufferSize * 2;
            }
        }

        private Text generateSentence(int noWords) {
            StringBuffer sentence = new StringBuffer();
            String space = " ";
            for (int i = 0; i < noWords; ++i) {
                sentence.append(words[random.nextInt(words.length)]);
                sentence.append(space);
            }
            return new Text(sentence.toString());
        }
    }


    static class RandomInputFormat extends InputFormat<Text, Text> {

        /**
         * Generate the requested number of file splits, with the filename
         * set to the filename of the output file.
         */
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> result = new ArrayList<InputSplit>();
            Path outDir = FileOutputFormat.getOutputPath(job);
            int numSplits =
                    job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            for (int i = 0; i < numSplits; ++i) {
                result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1,
                        (String[]) null));
            }
            return result;
        }

        /**
         * Return a single record (filename, "") where the filename is taken from
         * the file split.
         */
        static class RandomRecordReader extends RecordReader<Text, Text> {
            Path name;
            Text key = null;
            Text value = new Text();

            public RandomRecordReader(Path p) {
                name = p;
            }

            public void initialize(InputSplit split,
                                   TaskAttemptContext context)
                    throws IOException, InterruptedException {

            }

            public boolean nextKeyValue() {
                if (name != null) {
                    key = new Text();
                    key.set(name.getName());
                    name = null;
                    return true;
                }
                return false;
            }

            public Text getCurrentKey() {
                return key;
            }

            public Text getCurrentValue() {
                return value;
            }

            public void close() {
            }

            public float getProgress() {
                return 0.0f;
            }
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) throws IOException, InterruptedException {
            return new RandomRecordReader(((FileSplit) split).getPath());
        }
    }


    private static String[] words = {
            "diurnalness", "Homoiousian",
            "spiranthic", "tetragynian",
            "silverhead", "ungreat",
            "lithograph", "exploiter",
            "physiologian", "by",
            "hellbender", "Filipendula",
            "undeterring", "antiscolic",
            "pentagamist", "hypoid",
    };

}

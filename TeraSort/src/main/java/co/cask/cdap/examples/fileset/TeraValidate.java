package co.cask.cdap.examples.fileset;


import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.Unsigned16;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.PureJavaCrc32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.Checksum;

public class TeraValidate extends AbstractMapReduce {
    private static final Logger LOG = LoggerFactory.getLogger(TeraValidate.class);

    static final byte[] ONE = {'1'};
    static final byte[] TWO = {'2'};

    @UseDataSet("benchData")
    private Table benchData;

    @Override
    public void configure() {
        setInputDataset("counts");
        setOutputDataset("counts");
        setMapperResources(new Resources(1024));
        setReducerResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
        Job job = context.getHadoopJob();
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TeraInputFormat.class);
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);
        FileInputFormat.setMinInputSplitSize(job, Long.MAX_VALUE);
    }

    private static final Text ERROR = new Text("error");
    private static final Text CHECKSUM = new Text("checksum");

    private static String textifyBytes(Text t) {
        BytesWritable b = new BytesWritable();
        b.set(t.getBytes(), 0, t.getLength());
        return b.toString();
    }


    static class ValidateMapper extends Mapper<Text, Text, Text, Text> {
        private Text lastKey;
        private String filename;
        private Unsigned16 checksum = new Unsigned16();
        private Unsigned16 tmp = new Unsigned16();
        private Checksum crc32 = new PureJavaCrc32();

        /**
         * Get the final part of the input name
         *
         * @param split the input split
         * @return the "part-r-00000" for the input
         */
        private String getFilename(FileSplit split) {
            return split.getPath().getName();
        }

        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            if (lastKey == null) {
                FileSplit fs = (FileSplit) context.getInputSplit();
                filename = getFilename(fs);
                context.write(new Text(filename + ":begin"), key);
                lastKey = new Text();
            } else {
                if (key.compareTo(lastKey) < 0) {
                    context.write(ERROR, new Text("misorder in " + filename +
                            " between " + textifyBytes(lastKey) +
                            " and " + textifyBytes(key)));
                }
            }
            // compute the crc of the key and value and add it to the sum
            crc32.reset();
            crc32.update(key.getBytes(), 0, key.getLength());
            crc32.update(value.getBytes(), 0, value.getLength());
            tmp.set(crc32.getValue());
            checksum.add(tmp);
            lastKey.set(key);
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException {
            if (lastKey != null) {
                context.write(new Text(filename + ":end"), lastKey);
                context.write(CHECKSUM, new Text(checksum.toString()));
            }
        }
    }

    /**
     * Check the boundaries between the output files by making sure that the
     * boundary keys are always increasing.
     * Also passes any error reports along intact.
     */
    static class ValidateReducer extends Reducer<Text, Text, Text, Text> {
        private boolean firstKey = true;
        private Text lastKey = new Text();
        private Text lastValue = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            if (ERROR.equals(key)) {
                for (Text val : values) {
                    context.write(key, val);
                }
            } else if (CHECKSUM.equals(key)) {
                Unsigned16 tmp = new Unsigned16();
                Unsigned16 sum = new Unsigned16();
                for (Text val : values) {
                    tmp.set(val.toString());
                    sum.add(tmp);
                }
                context.write(CHECKSUM, new Text(sum.toString()));
            } else {
                Text value = values.iterator().next();
                if (firstKey) {
                    firstKey = false;
                } else {
                    if (value.compareTo(lastValue) < 0) {
                        context.write(ERROR,
                                new Text("bad key partitioning:\n  file " +
                                        lastKey + " key " +
                                        textifyBytes(lastValue) +
                                        "\n  file " + key + " key " +
                                        textifyBytes(value)));
                    }
                }
                lastKey.set(key);
                lastValue.set(value);
            }
        }

    }

}
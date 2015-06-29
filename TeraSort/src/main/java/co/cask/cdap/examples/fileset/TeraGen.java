package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.examples.terasort.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by peilunzh on 6/19/2015.
 */
public class TeraGen extends AbstractMapReduce {

    private static final Logger LOG = LoggerFactory.getLogger(TeraGen.class);
    private static long numBytesToWrite = 100 * 1024 * 1024;
    private static String BENCH_SIZE = "size";
    static final byte[] ONE = {'1'};
    static final byte[] THREE = {'3'};

    @UseDataSet("benchData")
    private Table benchData;

    @Override
    public void configure() {
        setOutputDataset("lines");
        setMapperResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
        Job job = context.getHadoopJob();
        job.setInputFormatClass(RangeInputFormat.class);
        job.setOutputFormatClass(TeraOutputFormat.class);
        job.setMapperClass(SortGenMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);


        String numOfRows = context.getRuntimeArguments().get("rows");
        if (numOfRows != null) {
            LOG.info("rows we get in config is : " + numOfRows);
            long totalBytes = Long.valueOf(numOfRows) * 100;
            setNumberOfRows(job, totalBytes / 100);
            benchData.put(new Put(ONE, THREE, totalBytes));
        }

    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    }


    /**
     * The Mapper class that given a row number, will generate the appropriate
     * output line.
     */
    public static class SortGenMapper
            extends Mapper<LongWritable, NullWritable, Text, Text> {

        private Text key = new Text();
        private Text value = new Text();
        private Unsigned16 rand = null;
        private Unsigned16 rowId = null;
        private Unsigned16 total = new Unsigned16();
        private static final Unsigned16 ONE = new Unsigned16(1);
        private byte[] buffer = new byte[TeraInputFormat.KEY_LENGTH +
                TeraInputFormat.VALUE_LENGTH];

        public void map(LongWritable row, NullWritable ignored,
                        Context context) throws IOException, InterruptedException {
            if (rand == null) {
                rowId = new Unsigned16(row.get());
                rand = Random16.skipAhead(rowId);
            }
            Random16.nextRand(rand);
            GenSort.generateRecord(buffer, rand, rowId);
            key.set(buffer, 0, TeraInputFormat.KEY_LENGTH);
            value.set(buffer, TeraInputFormat.KEY_LENGTH,
                    TeraInputFormat.VALUE_LENGTH);
            context.write(key, value);
            rowId.add(ONE);
        }


    }


    /**
     * An input format that assigns ranges of longs to each mapper.
     */
    static class RangeInputFormat
            extends InputFormat<LongWritable, NullWritable> {

        /**
         * An input split consisting of a range on numbers.
         */
        static class RangeInputSplit extends InputSplit implements Writable {
            long firstRow;
            long rowCount;

            public RangeInputSplit() {
            }

            public RangeInputSplit(long offset, long length) {
                firstRow = offset;
                rowCount = length;
            }

            public long getLength() throws IOException {
                return 0;
            }

            public String[] getLocations() throws IOException {
                return new String[]{};
            }

            public void readFields(DataInput in) throws IOException {
                firstRow = WritableUtils.readVLong(in);
                rowCount = WritableUtils.readVLong(in);
            }

            public void write(DataOutput out) throws IOException {
                WritableUtils.writeVLong(out, firstRow);
                WritableUtils.writeVLong(out, rowCount);
            }
        }

        /**
         * A record reader that will generate a range of numbers.
         */
        static class RangeRecordReader
                extends RecordReader<LongWritable, NullWritable> {
            long startRow;
            long finishedRows;
            long totalRows;
            LongWritable key = null;

            public RangeRecordReader() {
            }

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                startRow = ((RangeInputSplit) split).firstRow;
                finishedRows = 0;
                totalRows = ((RangeInputSplit) split).rowCount;
            }

            public void close() throws IOException {
                // NOTHING
            }

            public LongWritable getCurrentKey() {
                return key;
            }

            public NullWritable getCurrentValue() {
                return NullWritable.get();
            }

            public float getProgress() throws IOException {
                return finishedRows / (float) totalRows;
            }

            public boolean nextKeyValue() {
                if (key == null) {
                    key = new LongWritable();
                }
                if (finishedRows < totalRows) {
                    key.set(startRow + finishedRows);
                    finishedRows += 1;
                    return true;
                } else {
                    return false;
                }
            }

        }

        public RecordReader<LongWritable, NullWritable>
        createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            return new RangeRecordReader();
        }

        /**
         * Create the desired number of splits, dividing the number of rows
         * between the mappers.
         */
        public List<InputSplit> getSplits(JobContext job) {
            long totalRows = getNumberOfRows(job);
            int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            LOG.info("Generating " + totalRows + " using " + numSplits);
            List<InputSplit> splits = new ArrayList<InputSplit>();
            long currentRow = 0;
            for (int split = 0; split < numSplits; ++split) {
                long goal =
                        (long) Math.ceil(totalRows * (double) (split + 1) / numSplits);
                splits.add(new RangeInputSplit(currentRow, goal - currentRow));
                currentRow = goal;
            }
            return splits;
        }

    }

    static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong(TeraSortConfigKeys.NUM_ROWS.key(),
                TeraSortConfigKeys.DEFAULT_NUM_ROWS);
    }

    static void setNumberOfRows(Job job, long numRows) {
        job.getConfiguration().setLong(TeraSortConfigKeys.NUM_ROWS.key(), numRows);
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


}

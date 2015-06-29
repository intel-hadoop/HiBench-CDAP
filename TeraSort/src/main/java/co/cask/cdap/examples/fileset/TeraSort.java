package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.examples.terasort.TeraSortConfigKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by peilunzh on 6/24/2015.
 */
public class TeraSort extends AbstractMapReduce {

    private static final Logger LOG = LoggerFactory.getLogger(TeraSort.class);
    public static double startTime = 2;
    public static double endTime = 1;

    static final byte[] ONE = {'1'};
    static final byte[] TWO = {'2'};

    @UseDataSet("benchData")
    private Table benchData;

    @Override
    public void configure() {
        setInputDataset("lines");
        setOutputDataset("counts");
        setMapperResources(new Resources(1024));
        setReducerResources(new Resources(1024));
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
        startTime = System.currentTimeMillis();
        benchData.put(new Put(ONE, ONE, startTime));
        Job job = context.getHadoopJob();
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TeraInputFormat.class);
        job.setOutputFormatClass(TeraOutputFormat.class);
        //job.setMapperClass(Mapper.class);
        //job.setReducerClass(Reducer.class);
        job.setPartitionerClass(SimplePartitioner.class);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
        endTime = System.currentTimeMillis();
        benchData.put(new Put(ONE, TWO, endTime));
    }


    public static class DoMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value,
                        Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class DoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


    /**
     * A partitioner that splits text keys into roughly equal partitions
     * in a global sorted order.
     */

    public static class TotalOrderPartitioner extends Partitioner<Text, Text> {
        private TrieNode trie;
        private Text[] splitPoints;
        private Configuration conf;

        /**
         * A generic trie node
         */

        static abstract class TrieNode {
            private int level;

            TrieNode(int level) {
                this.level = level;
            }

            abstract int findPartition(Text key);

            abstract void print(PrintStream strm) throws IOException;

            int getLevel() {
                return level;
            }
        }

        /**
         * An inner trie node that contains 256 children based on the next
         * character.
         */

        static class InnerTrieNode extends TrieNode {
            private TrieNode[] child = new TrieNode[256];

            InnerTrieNode(int level) {
                super(level);
            }

            int findPartition(Text key) {
                int level = getLevel();
                if (key.getLength() <= level) {
                    return child[0].findPartition(key);
                }
                return child[key.getBytes()[level] & 0xff].findPartition(key);
            }

            void setChild(int idx, TrieNode child) {
                this.child[idx] = child;
            }

            void print(PrintStream strm) throws IOException {
                for (int ch = 0; ch < 256; ++ch) {
                    for (int i = 0; i < 2 * getLevel(); ++i) {
                        strm.print(' ');
                    }
                    strm.print(ch);
                    strm.println(" ->");
                    if (child[ch] != null) {
                        child[ch].print(strm);
                    }
                }
            }
        }

        /**
         * A leaf trie node that does string compares to figure out where the given
         * key belongs between lower..upper.
         */

        static class LeafTrieNode extends TrieNode {
            int lower;
            int upper;
            Text[] splitPoints;

            LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
                super(level);
                this.splitPoints = splitPoints;
                this.lower = lower;
                this.upper = upper;
            }

            int findPartition(Text key) {
                for (int i = lower; i < upper; ++i) {
                    if (splitPoints[i].compareTo(key) > 0) {
                        return i;
                    }
                }
                return upper;
            }

            void print(PrintStream strm) throws IOException {
                for (int i = 0; i < 2 * getLevel(); ++i) {
                    strm.print(' ');
                }
                strm.print(lower);
                strm.print(", ");
                strm.println(upper);
            }
        }


        /**
         * Read the cut points from the given sequence file.
         *
         * @param fs   the file system
         * @param p    the path to read
         * @param conf the job config
         * @return the strings to split the partitions on
         * @throws IOException
         */

        private static Text[] readPartitions(FileSystem fs, Path p,
                                             Configuration conf) throws IOException {
            int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
            Text[] result = new Text[reduces - 1];
            DataInputStream reader = fs.open(p);
            for (int i = 0; i < reduces - 1; ++i) {
                result[i] = new Text();
                result[i].readFields(reader);
            }
            reader.close();
            return result;
        }

        /**
         * Given a sorted set of cut points, build a trie that will find the correct
         * partition quickly.
         *
         * @param splits   the list of cut points
         * @param lower    the lower bound of partitions 0..numPartitions-1
         * @param upper    the upper bound of partitions 0..numPartitions-1
         * @param prefix   the prefix that we have already checked against
         * @param maxDepth the maximum depth we will build a trie for
         * @return the trie node that will divide the splits correctly
         */

        private static TrieNode buildTrie(Text[] splits, int lower, int upper,
                                          Text prefix, int maxDepth) {
            int depth = prefix.getLength();
            if (depth >= maxDepth || lower == upper) {
                return new LeafTrieNode(depth, splits, lower, upper);
            }
            InnerTrieNode result = new InnerTrieNode(depth);
            Text trial = new Text(prefix);
            // append an extra byte on to the prefix
            trial.append(new byte[1], 0, 1);
            int currentBound = lower;
            for (int ch = 0; ch < 255; ++ch) {
                trial.getBytes()[depth] = (byte) (ch + 1);
                lower = currentBound;
                while (currentBound < upper) {
                    if (splits[currentBound].compareTo(trial) >= 0) {
                        break;
                    }
                    currentBound += 1;
                }
                trial.getBytes()[depth] = (byte) ch;
                result.child[ch] = buildTrie(splits, lower, currentBound, trial,
                        maxDepth);
            }
            // pick up the rest
            trial.getBytes()[depth] = (byte) 255;
            result.child[255] = buildTrie(splits, currentBound, upper, trial,
                    maxDepth);
            return result;
        }

        public void setConf(Configuration conf) {
            try {
                FileSystem fs = FileSystem.getLocal(conf);
                this.conf = conf;
                Path partFile = new Path(TeraInputFormat.PARTITION_FILENAME);
                splitPoints = readPartitions(fs, partFile, conf);
                trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
            } catch (IOException ie) {
                throw new IllegalArgumentException("can't read partitions file", ie);
            }
        }

        public Configuration getConf() {
            return conf;
        }

        public TotalOrderPartitioner() {
        }

        public int getPartition(Text key, Text value, int numPartitions) {
            return trie.findPartition(key);
        }

    }

    /**
     * A total order partitioner that assigns keys based on their first
     * PREFIX_LENGTH bytes, assuming a flat distribution.
     */

    public static class SimplePartitioner extends Partitioner<Text, Text>
            implements Configurable {
        int prefixesPerReduce;
        private static final int PREFIX_LENGTH = 3;
        private Configuration conf = null;

        public void setConf(Configuration conf) {
            this.conf = conf;
            prefixesPerReduce = (int) Math.ceil((1 << (8 * PREFIX_LENGTH)) /
                    (float) conf.getInt(MRJobConfig.NUM_REDUCES, 1));
        }

        public Configuration getConf() {
            return conf;
        }

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            byte[] bytes = key.getBytes();
            int len = Math.min(PREFIX_LENGTH, key.getLength());
            int prefix = 0;
            for (int i = 0; i < len; ++i) {
                prefix = (prefix << 8) | (0xff & bytes[i]);
            }
            return prefix / prefixesPerReduce;
        }

    }

    public static boolean getUseSimplePartitioner(JobContext job) {
        return job.getConfiguration().getBoolean(
                TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(),
                TeraSortConfigKeys.DEFAULT_USE_SIMPLE_PARTITIONER);
    }

    public static void setUseSimplePartitioner(Job job, boolean value) {
        job.getConfiguration().setBoolean(
                TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(), value);
    }

    public static int getOutputReplication(JobContext job) {
        return job.getConfiguration().getInt(
                TeraSortConfigKeys.OUTPUT_REPLICATION.key(),
                TeraSortConfigKeys.DEFAULT_OUTPUT_REPLICATION);
    }

    public static void setOutputReplication(Job job, int value) {
        job.getConfiguration().setInt(TeraSortConfigKeys.OUTPUT_REPLICATION.key(),
                value);
    }


}

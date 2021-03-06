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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * An Application to run wordCount Benchmark
 */
public class WordCountApp extends AbstractApplication {

    @Override
    public void configure() {
        setName("WordCount");
        setDescription("Benchmark Application with workload wordCount");
        createDataset("lines", FileSet.class, FileSetProperties.builder()
                .setInputFormat(TextInputFormat.class)
                .setOutputFormat(TextOutputFormat.class)
                .setOutputProperty(TextOutputFormat.SEPERATOR, " ")
                .build());
        createDataset("counts", FileSet.class, FileSetProperties.builder()
                .setInputFormat(TextInputFormat.class)
                .setOutputFormat(TextOutputFormat.class)
                .setOutputProperty(TextOutputFormat.SEPERATOR, " ")
                .build());
        createDataset("benchData", Table.class);
        addService(new FileSetService());
        addMapReduce(new RandomTextWriter());
        addMapReduce(new WordCount());
        addService(new BenchService());
        addService(new BenchUI());
    }
}

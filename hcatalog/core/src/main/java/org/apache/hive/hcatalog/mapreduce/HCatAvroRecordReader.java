package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.LazyHCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HCatAvroRecordReader  extends RecordReader<WritableComparable, AvroGenericRecordWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(HCatAvroRecordReader.class);

    WritableComparable currentKey;
    AvroGenericRecordWritable currentValue;

    org.apache.hadoop.mapred.RecordReader<WritableComparable,Writable> baseRecordReader;

    private final HiveStorageHandler storageHandler;
    private Map<String, Object> valuesNotInDataCols;

    /**
     * Instantiates a new hcat record reader.
     */
    public HCatAvroRecordReader(HiveStorageHandler storageHandler,
                            Map<String, Object> valuesNotInDataCols) {
        this.storageHandler = storageHandler;
        this.valuesNotInDataCols = valuesNotInDataCols;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        HCatSplit hcatSplit = InternalUtil.castToHCatSplit(inputSplit);

        baseRecordReader = createBaseRecordReader(hcatSplit, storageHandler, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentKey == null) {
            currentKey = baseRecordReader.createKey();
            currentValue = (AvroGenericRecordWritable) baseRecordReader.createValue();
        }

        while (baseRecordReader.next(currentKey, currentValue)) {
            return true;
        }

        return false;
    }

    @Override
    public WritableComparable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public AvroGenericRecordWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        try {
            return baseRecordReader.getProgress();
        } catch (IOException e) {
            LOG.warn("Exception in HCatRecord reader", e);
        }
        return 0.0f; // errored
    }

    @Override
    public void close() throws IOException {

    }

    private org.apache.hadoop.mapred.RecordReader createBaseRecordReader(HCatSplit hcatSplit,
                                                                         HiveStorageHandler storageHandler, TaskAttemptContext taskContext) throws IOException {

        JobConf jobConf = HCatUtil.getJobConfFromContext(taskContext);
        HCatUtil.copyJobPropertiesToJobConf(hcatSplit.getPartitionInfo().getJobProperties(), jobConf);
        org.apache.hadoop.mapred.InputFormat inputFormat =
                HCatInputFormat.getMapRedInputFormat(jobConf, storageHandler.getInputFormatClass());
        return inputFormat.getRecordReader(hcatSplit.getBaseSplit(), jobConf,
                InternalUtil.createReporter(taskContext));
    }
}

package org.apache.hive.hcatalog.mapreduce;

import com.google.common.base.Preconditions;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.avro.AvroSpecificRecordWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HcatAvroInputFormat<T extends SpecificRecord> extends InputFormat<WritableComparable, AvroSpecificRecordWritable<T>> {


    private Configuration conf;
    private InputJobInfo inputJobInfo;

    /**
     * Gets the InputJobInfo object by reading the Configuration and deserializing
     * the string. If InputJobInfo is not present in the configuration, throws an
     * exception since that means HCatInputFormat.setInput has not been called.
     * @param conf the Configuration object
     * @return the InputJobInfo object
     * @throws IOException the exception
     */
    private static InputJobInfo getJobInfo(Configuration conf)
            throws IOException {
        String jobString = conf.get(
                HCatConstants.HCAT_KEY_JOB_INFO);
        if (jobString == null) {
            throw new IOException("job information not found in JobContext."
                    + " HCatInputFormat.setInput() not called?");
        }

        return (InputJobInfo) HCatUtil.deserialize(jobString);
    }

    private void setInputPath(JobConf jobConf, String location)
            throws IOException {

        // ideally we should just call FileInputFormat.setInputPaths() here - but
        // that won't work since FileInputFormat.setInputPaths() needs
        // a Job object instead of a JobContext which we are handed here

        int length = location.length();
        int curlyOpen = 0;
        int pathStart = 0;
        boolean globPattern = false;
        List<String> pathStrings = new ArrayList<String>();

        for (int i = 0; i < length; i++) {
            char ch = location.charAt(i);
            switch (ch) {
                case '{': {
                    curlyOpen++;
                    if (!globPattern) {
                        globPattern = true;
                    }
                    break;
                }
                case '}': {
                    curlyOpen--;
                    if (curlyOpen == 0 && globPattern) {
                        globPattern = false;
                    }
                    break;
                }
                case ',': {
                    if (!globPattern) {
                        pathStrings.add(location.substring(pathStart, i));
                        pathStart = i + 1;
                    }
                    break;
                }
            }
        }
        pathStrings.add(location.substring(pathStart, length));

        Path[] paths = StringUtils.stringToPath(pathStrings.toArray(new String[0]));
        String separator = "";
        StringBuilder str = new StringBuilder();

        for (Path path : paths) {
            FileSystem fs = path.getFileSystem(jobConf);
            final String qualifiedPath = fs.makeQualified(path).toString();
            str.append(separator)
                    .append(StringUtils.escapeString(qualifiedPath));
            separator = StringUtils.COMMA_STR;
        }

        jobConf.set("mapred.input.dir", str.toString());
    }

    /**
     * Logically split the set of input files for the job. Returns the
     * underlying InputFormat's splits
     * @param jobContext the job context object
     * @return the splits, an HCatInputSplit wrapper over the storage
     *         handler InputSplits
     * @throws IOException or InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        // Set up recursive reads for sub-directories.
        // (Otherwise, sub-directories produced by Hive UNION operations won't be readable.)
        conf.setBoolean("mapred.input.dir.recursive", true);

        //Get the job info from the configuration,
        //throws exception if not initialized
        InputJobInfo inputJobInfo;
        try {
            inputJobInfo = getJobInfo(conf);
        } catch (Exception e) {
            throw new IOException(e);
        }

        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<PartInfo> partitionInfoList = inputJobInfo.getPartitions();
        if (partitionInfoList == null) {
            //No partitions match the specified partition filter
            return splits;
        }

        HiveStorageHandler storageHandler;
        //For each matching partition, call getSplits on the underlying InputFormat
        for (PartInfo partitionInfo : partitionInfoList) {
            JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
            setInputPath(jobConf, partitionInfo.getLocation());
            Map<String, String> jobProperties = partitionInfo.getJobProperties();

            HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);
            HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

            storageHandler = HCatUtil.getStorageHandler(
                    jobConf, partitionInfo);

            //Get the input format
            Class inputFormatClass = storageHandler.getInputFormatClass();
            org.apache.hadoop.mapred.InputFormat inputFormat = HCatBaseInputFormat.getMapRedInputFormat(jobConf, inputFormatClass);

            //Call getSplit on the InputFormat, create an HCatSplit for each
            //underlying split. When the desired number of input splits is missing,
            //use a default number (denoted by zero).
            //TODO(malewicz): Currently each partition is split independently into
            //a desired number. However, we want the union of all partitions to be
            //split into a desired number while maintaining balanced sizes of input
            //splits.
            int desiredNumSplits =
                    conf.getInt(HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, 0);
            org.apache.hadoop.mapred.InputSplit[] baseSplits =
                    inputFormat.getSplits(jobConf, desiredNumSplits);

            for (org.apache.hadoop.mapred.InputSplit split : baseSplits) {
                splits.add(new HCatSplit(partitionInfo, split));
            }
        }

        return splits;
    }

    @Override
    public RecordReader<WritableComparable, AvroSpecificRecordWritable<T>> createRecordReader(org.apache.hadoop.mapreduce
                                                                                                        .InputSplit inputSplit,
                                                                  TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        HCatSplit hcatSplit = InternalUtil.castToHCatSplit(inputSplit);
        PartInfo partitionInfo = hcatSplit.getPartitionInfo();
        // Ensure PartInfo's TableInfo is initialized.
        if (partitionInfo.getTableInfo() == null) {
            partitionInfo.setTableInfo(((InputJobInfo)HCatUtil.deserialize(
                    taskAttemptContext.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO)
            )).getTableInfo());
        }
        JobContext jobContext = taskAttemptContext;
        Configuration conf = jobContext.getConfiguration();

        HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(
                conf, partitionInfo);

        JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
        Map<String, String> jobProperties = partitionInfo.getJobProperties();
        HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

        return new HCatAvroRecordReader<T>(storageHandler, null);
    }


    /**
     * Initializes the input with a null filter.
     * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
     */
    public static HcatAvroInputFormat setInput(
            Job job, String dbName, String tableName)
            throws IOException {
        return setInput(job.getConfiguration(), dbName, tableName, null);
    }

    /**
     * Initializes the input with a provided filter.
     * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
     */
    public static HcatAvroInputFormat setInput(
            Job job, String dbName, String tableName, String filter)
            throws IOException {
        return setInput(job.getConfiguration(), dbName, tableName, filter);
    }

    /**
     * Initializes the input with a null filter.
     * See {@link #setInput(org.apache.hadoop.conf.Configuration, String, String, String)}
     */
    public static HcatAvroInputFormat setInput(
            Configuration conf, String dbName, String tableName)
            throws IOException {
        return setInput(conf, dbName, tableName, null);
    }

    /**
     * Set inputs to use for the job. This queries the metastore with the given input
     * specification and serializes matching partitions into the job conf for use by MR tasks.
     * @param conf the job configuration
     * @param dbName database name, which if null 'default' is used
     * @param tableName table name
     * @param filter the partition filter to use, can be null for no filter
     * @throws IOException on all errors
     */
    public static HcatAvroInputFormat setInput(
            Configuration conf, String dbName, String tableName, String filter)
            throws IOException {

        Preconditions.checkNotNull(conf, "required argument 'conf' is null");
        Preconditions.checkNotNull(tableName, "required argument 'tableName' is null");

        HcatAvroInputFormat hCatInputFormat = new HcatAvroInputFormat();
        hCatInputFormat.conf = conf;
        hCatInputFormat.inputJobInfo = InputJobInfo.create(dbName, tableName, filter, null);

        try {
            InitializeInput.setInput(conf, hCatInputFormat.inputJobInfo);
        } catch (Exception e) {
            throw new IOException(e);
        }

        return hCatInputFormat;
    }
}

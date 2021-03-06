/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.mapper.BulkImportMapper;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;

/** Builds a job that runs a KijiBulkImporter to import data into a Kiji table. */
@ApiAudience.Public
public final class KijiBulkImportJobBuilder
    extends KijiMapReduceJobBuilder<KijiBulkImportJobBuilder> {

  /** The class of the bulk importer to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends KijiBulkImporter> mBulkImporterClass;

  /** The bulk importer instance. */
  private KijiBulkImporter<?, ?> mBulkImporter;
  /** The mapper instance to run (which runs the bulk importer inside it). */
  private KijiMapper<?, ?, ?, ?> mMapper;
  /** The reducer instance to run (may be null). */
  private KijiReducer<?, ?, ?, ?> mReducer;

  /** The job input. */
  private MapReduceJobInput mJobInput;
  /** The target kiji table for the import. */
  private HBaseKijiTable mOutputTable;

  /** Constructs a builder for jobs that run a KijiBulkImporter. */
  private KijiBulkImportJobBuilder() {
    mBulkImporterClass = null;

    mBulkImporter = null;
    mMapper = null;
    mReducer = null;

    mJobInput = null;
    mOutputTable = null;
  }

  /**
   * Creates a new builder for Kiji bulk import jobs.
   *
   * @return a new Kiji bulk import job builder.
   */
  public static KijiBulkImportJobBuilder create() {
    return new KijiBulkImportJobBuilder();
  }

  /**
   * Configures the job with input.
   *
   * @param jobInput The input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiBulkImportJobBuilder withInput(MapReduceJobInput jobInput) {
    mJobInput = jobInput;
    return this;
  }

  /**
   * Configures the job with a bulk importer to run in the map phase.
   *
   * @param bulkImporterClass The bulk importer class to use in the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public KijiBulkImportJobBuilder withBulkImporter(
      Class<? extends KijiBulkImporter> bulkImporterClass) {
    mBulkImporterClass = bulkImporterClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Store the name of the the importer to use in the job configuration so the mapper can
    // create instances of it.
    job.getConfiguration().setClass(
        KijiConfKeys.KIJI_BULK_IMPORTER_CLASS, mBulkImporterClass, KijiBulkImporter.class);

    // Make sure the job output format is a KijiTableMapReduceJobOutput or a subclass of it.
    MapReduceJobOutput jobOutput = getJobOutput();
    if (!(jobOutput instanceof KijiTableMapReduceJobOutput)) {
      throw new JobConfigurationException(
          "Job output must be a KijiTableMapReduceJobOutput or a subclass.");
    }
    mOutputTable = HBaseKijiTable.downcast(((KijiTableMapReduceJobOutput) jobOutput).getTable());
    jobOutput.configure(job);

    // Construct the bulk importer instance.
    if (null == mBulkImporterClass) {
      throw new JobConfigurationException("Must specify a bulk importer.");
    }
    mBulkImporter = ReflectionUtils.newInstance(mBulkImporterClass, job.getConfiguration());

    // Configure the mapper and reducer. This part depends on whether we're going to write
    // to HFiles or directly to the table.
    configureJobForHFileOutput(job);

    job.setJobName("Kiji bulk import: " + mBulkImporterClass.getSimpleName());

    // Configure the MapReduce job.
    super.configureJob(job);
  }

  /**
   * Configures the job settings specific to writing HFiles.
   *
   * @param job The job to configure.
   */
  protected void configureJobForHFileOutput(Job job) {
    // Construct the mapper instance that runs the importer.
    mMapper = new BulkImportMapper<Object, Object>();

    // Don't need to do anything during the Reducer, but we need to run the reduce phase
    // so the KeyValue records output from the map phase get sorted.
    mReducer = new IdentityReducer<Object, Object>();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJob build(Job job) {
    return KijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mBulkImporter.getRequiredStores();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJobInput getJobInput() {
    return mJobInput;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getCombiner() {
    // Use no combiner.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mBulkImporterClass;
  }

  /** {@inheritDoc} */
  @Override
  protected Kiji getKiji() {
    return mOutputTable.getKiji();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiTableLayout getTableLayout() {
    return mOutputTable.getLayout();
  }
}

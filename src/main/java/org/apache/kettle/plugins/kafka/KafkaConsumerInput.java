/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.kettle.plugins.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.CurrentDirectoryResolver;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.SingleThreadedTransExecutor;
import org.pentaho.di.trans.StepWithMappingMeta;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.injector.InjectorMeta;
import org.pentaho.di.trans.steps.recordsfromstream.RecordsFromStreamMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Consume messages from a Kafka topic
 */
public class KafkaConsumerInput extends BaseStep implements StepInterface {

  private static Class<?> PKG = KafkaConsumerInputMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public KafkaConsumerInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                             Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface The metadata to work with
   * @param stepDataInterface The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    KafkaConsumerInputMeta meta = (KafkaConsumerInputMeta) stepMetaInterface;
    KafkaConsumerInputData data = (KafkaConsumerInputData) stepDataInterface;

    boolean superInit = super.init( meta, data );
    if ( !superInit ) {
      logError( BaseMessages.getString( PKG, "KafkaConsumerInput.Error.InitFailed" ) );
      return false;
    }

    try {
      data.outputRowMeta = meta.getRowMeta( getStepname(), this );
    } catch ( KettleStepException e ) {
      log.logError( e.getMessage(), e );
    }

    data.batch = Const.toInt( environmentSubstitute( meta.getBatchSize() ), -1 );

    data.consumer = buildKafkaConsumer(this, meta);

    // Subscribe to the topics...
    //
    Set<String> topics = meta.getTopics().stream().map( this::environmentSubstitute ).collect( Collectors.toSet() );
    data.consumer.subscribe( topics );

    // Load and start the single threader transformation
    //
    try {
      initSubtransformation(meta, data);
    } catch(Exception e) {
      logError( "Error initializing sub-transformation", e );
      return false;
    }

    return true;
  }

  private void initSubtransformation(KafkaConsumerInputMeta meta, KafkaConsumerInputData data) throws KettleException {
    try {

      CurrentDirectoryResolver r = new CurrentDirectoryResolver();
      String realFilename = environmentSubstitute( meta.getFileName() );
      TransMeta subTransMeta = new TransMeta( realFilename, metaStore, repository, true, this, null );
      StepWithMappingMeta.replaceVariableValues( subTransMeta, this );
      StepWithMappingMeta.addMissingVariables( subTransMeta, this );
      subTransMeta.activateParameters();
      subTransMeta.setRepository( repository );
      subTransMeta.setMetaStore( metaStore );
      subTransMeta.setFilename( meta.getFileName() );
      subTransMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );
      logDetailed( "Loaded sub-transformation '"+realFilename+"'" );

      Trans subTrans = new Trans(subTransMeta, getTrans());
      subTrans.prepareExecution( getTrans().getArguments() );
      subTrans.setLogLevel( getTrans().getLogLevel() );
      subTrans.setPreviousResult( new Result(  ) );
      logDetailed( "Initialized sub-transformation '"+realFilename+"'" );

      // Find the "Get Record from Stream" step
      //
      for ( StepMetaDataCombi combi : subTrans.getSteps()) {
        if (combi.meta instanceof InjectorMeta ) {
          // Attach an injector to this step
          //
          data.rowProducer = subTrans.addRowProducer( combi.stepname, 0 );
          break;
        }
      }

      // See if we need to grab result records from the sub-transformation...
      //
      if (StringUtils.isNotEmpty(meta.getSubStep())) {
        StepInterface step = subTrans.findRunThread( meta.getSubStep() );
        if (step==null) {
          throw new KettleException( "Unable to find step '"+meta.getSubStep()+"' to retrieve rows from" );
        }
        step.addRowListener( new RowAdapter() {

          @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
            // Write this row to the next step(s)
            //
            KafkaConsumerInput.this.putRow(rowMeta, row);
          }
        } );
      }

      subTrans.startThreads();

      data.executor = new SingleThreadedTransExecutor( subTrans );

      // Initialize the sub-transformation
      //
      boolean ok = data.executor.init();
      if (!ok) {
        throw new KettleException( "Initialization of sub-transformation failed" );
      }

      getTrans().addActiveSubTransformation( getStepname(), subTrans );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to load and initialize sub transformation", e );
    }
  }

  @Override public void dispose( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    KafkaConsumerInputMeta meta = (KafkaConsumerInputMeta) stepMetaInterface;
    KafkaConsumerInputData data = (KafkaConsumerInputData) stepDataInterface;

    if (data.consumer!=null) {
      data.consumer.unsubscribe();
      data.consumer.close();
    }
    super.dispose( stepMetaInterface, stepDataInterface );
  }

  public static Consumer buildKafkaConsumer( VariableSpace space, KafkaConsumerInputMeta meta ) {

    Thread.currentThread().setContextClassLoader(null);

    Properties config = new Properties();

    // Set all the configuration options...
    //
    for (String option : meta.getConfig().keySet()) {
      String value = space.environmentSubstitute( meta.getConfig().get( option ) );
      if ( StringUtils.isNotEmpty( value ) ) {
        config.put(option, space.environmentSubstitute( value) );
      }
    }

    // The basics
    //
    config.put( ConsumerConfig.GROUP_ID_CONFIG, space.environmentSubstitute( Const.NVL(meta.getConsumerGroup(), "kettle") ));
    config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, space.environmentSubstitute( meta.getDirectBootstrapServers() ));
    config.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false );

    // Timeout : max batch wait
    //
    int timeout = Const.toInt(space.environmentSubstitute( meta.getBatchDuration()), 0);
    if (timeout>0) {
      config.put( ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, timeout);
    }

    // The batch size : max poll size
    //
    int batch = Const.toInt( space.environmentSubstitute( meta.getBatchSize() ), 0 );
    if (batch>0) {
      config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batch);
    }

    // Serializers...
    //
    String keySerializerClass = meta.getKeyField().getOutputType().getKafkaDeserializerClass();
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializerClass);
    String valueSerializerClass = meta.getMessageField().getOutputType().getKafkaDeserializerClass();
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializerClass);

    // Other options?

    return new KafkaConsumer( config );
  }

  @Override public boolean processRow( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) throws KettleException {
    KafkaConsumerInputMeta meta = (KafkaConsumerInputMeta) stepMetaInterface;
    KafkaConsumerInputData data = (KafkaConsumerInputData) stepDataInterface;

    // Poll records...
    // If we get any, process them...
    //
    ConsumerRecords<String, String> records = data.consumer.poll( data.batch > 0 ? data.batch : Long.MAX_VALUE );

    if (records.isEmpty()) {
      // We ca`n just skip this one, poll again next iteration of this method
      //
    } else {
      // Grab the records...
      //
      List<Object[]> rows = new ArrayList<>();
      for ( ConsumerRecord<String, String> record : records ) {
        Object[] outputRow = processMessageAsRow( meta, data, record );
        data.rowProducer.putRow( data.outputRowMeta, outputRow);
        incrementLinesInput();
      }

      // Pass them to the single threaded transformation and do an iteration...
      //
      data.executor.oneIteration();

      if (data.executor.isStopped() || data.executor.getErrors()>0) {
        // An error occurred in the sub-transformation
        //
        data.executor.getTrans().stopAll();
        setOutputDone();
        stopAll();
        return false;
      }

      // Confirm everything is processed correctly...
      //
      data.consumer.commitAsync();
    }

    return true;
  }

  public Object[] processMessageAsRow( KafkaConsumerInputMeta meta, KafkaConsumerInputData data, ConsumerRecord<String, String> record ) {

    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

    int index = 0;
    rowData[ index++ ] = record.key();
    rowData[ index++ ] = record.value();
    rowData[ index++ ] = record.topic();
    rowData[ index++ ] = (long) record.partition();
    rowData[ index++ ] = record.offset();
    rowData[ index++ ] = record.timestamp();

    return rowData;
  }
}

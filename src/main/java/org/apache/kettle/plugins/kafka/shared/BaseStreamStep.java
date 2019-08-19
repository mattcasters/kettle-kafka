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

package org.apache.kettle.plugins.kafka.shared;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;
import org.pentaho.di.trans.streaming.api.StreamSource;
import org.pentaho.di.trans.streaming.api.StreamWindow;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@SuppressWarnings ( "WeakerAccess" )
public class BaseStreamStep extends BaseStep {

  private static final Class<?> PKG = org.pentaho.di.trans.streaming.common.BaseStreamStep.class;
  protected BaseStreamStepMeta variablizedStepMeta;

  protected SubtransExecutor subtransExecutor;
  protected StreamWindow<List<Object>, Result> window;
  protected StreamSource<List<Object>> source;

  public BaseStreamStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                         TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    Preconditions.checkNotNull( stepMetaInterface );
    variablizedStepMeta = (BaseStreamStepMeta) stepMetaInterface;
    variablizedStepMeta.setParentStepMeta( getStepMeta() );
    variablizedStepMeta.setFileName( variablizedStepMeta.getTransformationPath() );

    boolean superInit = super.init( stepMetaInterface, stepDataInterface );

    try {
      TransMeta transMeta = TransExecutorMeta
        .loadMappingMeta( variablizedStepMeta, getTransMeta().getRepository(), getTransMeta().getMetaStore(),
          getParentVariableSpace() );
      variablizedStepMeta = (BaseStreamStepMeta) variablizedStepMeta.withVariables( this );
      subtransExecutor = new SubtransExecutor( getStepname(),
        getTrans(), transMeta, true,
        new TransExecutorParameters(), variablizedStepMeta.getSubStep() );

    } catch ( KettleException e ) {
      log.logError( e.getLocalizedMessage(), e );
      return false;
    }

    List<CheckResultInterface> remarks = new ArrayList<>();
    variablizedStepMeta.check(
      remarks, getTransMeta(), variablizedStepMeta.getParentStepMeta(),
      null, null, null, null, //these parameters are not used inside the method
      variables, getRepository(), getMetaStore() );
    boolean errorsPresent =
      remarks.stream().filter( result -> result.getType() == CheckResultInterface.TYPE_RESULT_ERROR )
        .peek( result -> logError( result.getText() ) )
        .count() > 0;
    if ( errorsPresent ) {
      return false;
    }
    return superInit;
  }


  @Override public void setOutputDone() {
    if ( !safeStopped.get() ) {
      super.setOutputDone();
    }
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Preconditions.checkArgument( first,
      BaseMessages.getString( PKG, "BaseStreamStep.ProcessRowsError" ) );
    Preconditions.checkNotNull( source );
    Preconditions.checkNotNull( window );

    source.open();

    bufferStream().forEach( result -> {
      if ( result.isSafeStop() ) {
        getTrans().safeStop();
      }

      putRows( result.getRows() );
    } );
    super.setOutputDone();

    // Needed for when an Abort Step is used.
    source.close();
    return false;
  }

  private Iterable<Result> bufferStream() {
    return window.buffer( source.observable() );
  }

  @Override
  public void stopRunning( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface )
    throws KettleException {
    if ( !safeStopped.get() ) {
      subtransExecutor.stop();
    }
    if ( source != null ) {
      source.close();
    }
    super.stopRunning( stepMetaInterface, stepDataInterface );
  }

  @Override public void resumeRunning() {
    if ( source != null ) {
      source.resume();
    }
    super.resumeRunning();
  }

  @Override public void pauseRunning() {
    if ( source != null ) {
      source.pause();
    }
    super.pauseRunning();
  }

  private void putRows( List<RowMetaAndData> rows ) {
    if ( isStopped() && !safeStopped.get() ) {
      return;
    }
    rows.forEach( row -> {
      try {
        putRow( row.getRowMeta(), row.getData() );
      } catch ( KettleStepException e ) {
        Throwables.propagate( e );
      }
    } );
  }

  protected int getBatchSize() {
    try {
      return Integer.parseInt( variablizedStepMeta.getBatchSize() );
    } catch ( NumberFormatException nfe ) {
      return 50;
    }
  }

  protected long getDuration() {
    try {
      return Long.parseLong( variablizedStepMeta.getBatchDuration() );
    } catch ( NumberFormatException nfe ) {
      return 5000L;
    }
  }

  @Override public Collection<StepStatus> subStatuses() {
    return subtransExecutor != null ? subtransExecutor.getStatuses().values() : Collections.emptyList();
  }

  @VisibleForTesting
  public StreamSource<List<Object>> getSource() {
    return source;
  }

  @VisibleForTesting
  public void setSource( StreamSource<List<Object>> source ) {
    this.source = source;
  }
}

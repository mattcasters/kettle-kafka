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

import com.google.common.base.Strings;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.di.trans.StepWithMappingMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseStreamStepMeta extends StepWithMappingMeta implements StepMetaInterface {


  private static final Class<?> PKG = BaseStreamStep.class;  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  public static final String TRANSFORMATION_PATH = "TRANSFORMATION_PATH";
  public static final String NUM_MESSAGES = "NUM_MESSAGES";
  public static final String DURATION = "DURATION";
  public static final String SUB_STEP = "SUB_STEP";

  @Injection( name = TRANSFORMATION_PATH )
  protected String transformationPath = "";

  @Injection( name = NUM_MESSAGES )
  protected String batchSize = "1000";

  @Injection( name = DURATION )
  protected String batchDuration = "1000";

  @Injection( name = SUB_STEP )
  protected String subStep = "";

  MappingMetaRetriever mappingMetaRetriever = TransExecutorMeta::loadMappingMeta;

  @FunctionalInterface interface MappingMetaRetriever {
    TransMeta get( StepWithMappingMeta mappingMeta, Repository rep, IMetaStore metaStore, VariableSpace space )
      throws KettleException;
  }

  public String getSubStep() {
    return subStep == null ? "" : subStep;
  }

  public void setSubStep( String subStep ) {
    this.subStep = subStep;
  }

  public void setTransformationPath( String transformationPath ) {
    this.transformationPath = transformationPath;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public void setBatchDuration( String batchDuration ) {
    this.batchDuration = batchDuration;
  }

  @Override public void setDefault() {
    batchSize = "1000";
    batchDuration = "1000";
  }

  public String getTransformationPath() {
    return transformationPath;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public String getBatchDuration() {
    return batchDuration;
  }

  @Override public void replaceFileName( String fileName ) {
    super.replaceFileName( fileName );
    setTransformationPath( fileName );
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    long duration = Long.MIN_VALUE;
    try {
      duration = Long.parseLong( space.environmentSubstitute( getBatchDuration() ) );
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "BaseStreamStepMeta.CheckResult.NaN", "Duration" ),
        stepMeta ) );
    }

    long size = Long.MIN_VALUE;
    try {
      size = Long.parseLong( space.environmentSubstitute( getBatchSize() ) );
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "BaseStreamStepMeta.CheckResult.NaN", "Number of records" ),
        stepMeta ) );
    }

    if ( duration == 0 && size == 0 ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "BaseStreamStepMeta.CheckResult.NoBatchDefined" ),
        stepMeta ) );
    }
  }

  @Override
  public String getFileName() {
    return ( Strings.isNullOrEmpty( this.fileName ) ? this.getTransformationPath() : this.fileName );
  }

  @Override
  public List<ResourceReference> getResourceDependencies( TransMeta transMeta, StepMeta stepInfo ) {
    List<ResourceReference> references = new ArrayList<>( 5 );
    String realFilename = transMeta.environmentSubstitute( transformationPath );
    ResourceReference reference = new ResourceReference( stepInfo );
    references.add( reference );

    if ( !Utils.isEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this step
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceEntry.ResourceType.ACTIONFILE ) );
    }

    return references;
  }

  @Override public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString( PKG, "BaseStreamStepMeta.ReferencedObject.SubTrans.Description" ) };
  }

  @Override public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { !Utils.isEmpty( transformationPath ) };
  }

  @Override public Object loadReferencedObject( int index, Repository rep, IMetaStore metaStore, VariableSpace space )
    throws KettleException {
    return loadMappingMeta( this, rep, metaStore, space );
  }

  public abstract RowMeta getRowMeta( String origin, VariableSpace space ) throws KettleStepException;

  @Override public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                                   VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    try {
      TransMeta transMeta = mappingMetaRetriever.get( this, repository, metaStore, space );
      if ( !StringUtil.isEmpty( getSubStep() ) ) {
        String realSubStepName = space.environmentSubstitute( getSubStep() );
        rowMeta.addRowMeta( transMeta.getPrevStepFields( realSubStepName ) );
        transMeta.getSteps().stream().filter( stepMeta -> stepMeta.getName().equals( realSubStepName ) )
          .findFirst()
          .ifPresent( stepMeta ->
          {
            try {
              stepMeta.getStepMetaInterface()
                .getFields( rowMeta, origin, info, nextStep, space, repository, metaStore );
            } catch ( KettleStepException e ) {
              throw new RuntimeException( e );
            }
          } );
      }
    } catch ( KettleException e ) {
      getLog().logDebug( "could not get fields, probable AEL" );
      rowMeta.addRowMeta( getRowMeta( origin, space ) );
    }
  }
}

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Strings.isNullOrEmpty;

@Step( id = "KettleKafkaConsumerInput", image = "KafkaConsumerInput.svg",
  i18nPackageName = "org.apache.kettle.plugins.kafka",
  name = "KafkaConsumer.TypeLongDesc",
  description = "KafkaConsumer.TypeTooltipDesc",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming",
  documentationUrl = "Products/Data_Integration/Transformation_Step_Reference/Kafka_Consumer" )
@InjectionSupported( localizationPrefix = "KafkaConsumerInputMeta.Injection.", groups = { "CONFIGURATION_PROPERTIES" } )
public class KafkaConsumerInputMeta extends BaseStreamStepMeta implements StepMetaInterface {

  public static final String CLUSTER_NAME = "clusterName";
  public static final String TOPIC = "topic";
  public static final String CONSUMER_GROUP = "consumerGroup";
  public static final String TRANSFORMATION_PATH = "transformationPath";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION = "batchDuration";
  public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";
  public static final String TOPIC_FIELD_NAME = TOPIC;
  public static final String OFFSET_FIELD_NAME = "offset";
  public static final String PARTITION_FIELD_NAME = "partition";
  public static final String TIMESTAMP_FIELD_NAME = "timestamp";
  public static final String OUTPUT_FIELD_TAG_NAME = "OutputField";
  public static final String KAFKA_NAME_ATTRIBUTE = "kafkaName";
  public static final String TYPE_ATTRIBUTE = "type";

  private static Class<?> PKG = KafkaConsumerInput.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  @Injection( name = "DIRECT_BOOTSTRAP_SERVERS" )
  private String directBootstrapServers;

  @Injection( name = "TOPICS" )
  private List<String> topics = new ArrayList<>();

  @Injection( name = "CONSUMER_GROUP" )
  private String consumerGroup;

  @InjectionDeep( prefix = "KEY" )
  private KafkaConsumerField keyField;

  @InjectionDeep( prefix = "MESSAGE" )
  private KafkaConsumerField messageField;

  @Injection( name = "NAMES", group = "CONFIGURATION_PROPERTIES" )
  protected transient List<String> injectedConfigNames;

  @Injection( name = "VALUES", group = "CONFIGURATION_PROPERTIES" )
  protected transient List<String> injectedConfigValues;

  private Map<String, String> config = new LinkedHashMap<>();

  private KafkaConsumerField topicField;

  private KafkaConsumerField offsetField;

  private KafkaConsumerField partitionField;

  private KafkaConsumerField timestampField;


  public KafkaConsumerInputMeta() {
    super(); // allocate BaseStepMeta
    keyField = new KafkaConsumerField(
      KafkaConsumerField.Name.KEY,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.KeyField" )
    );

    messageField = new KafkaConsumerField(
      KafkaConsumerField.Name.MESSAGE,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.MessageField" )
    );

    topicField = new KafkaConsumerField(
      KafkaConsumerField.Name.TOPIC,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.TopicField" )
    );

    partitionField = new KafkaConsumerField(
      KafkaConsumerField.Name.PARTITION,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.PartitionField" ),
      KafkaConsumerField.Type.Integer
    );

    offsetField = new KafkaConsumerField(
      KafkaConsumerField.Name.OFFSET,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OffsetField" ),
      KafkaConsumerField.Type.Integer
    );

    timestampField = new KafkaConsumerField(
      KafkaConsumerField.Name.TIMESTAMP,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.TimestampField" ),
      KafkaConsumerField.Type.Integer
    );
    setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) {
    readData( stepnode );
  }

  private void readData( Node stepnode ) {
    List<Node> topicsNode = XMLHandler.getNodes( stepnode, TOPIC );
    topicsNode.forEach( node -> {
      String displayName = XMLHandler.getNodeValue( node );
      addTopic( displayName );
    } );

    setConsumerGroup( XMLHandler.getTagValue( stepnode, CONSUMER_GROUP ) );
    setTransformationPath( XMLHandler.getTagValue( stepnode, TRANSFORMATION_PATH ) );
    String subStepTag = XMLHandler.getTagValue( stepnode, SUB_STEP );
    if ( !StringUtil.isEmpty( subStepTag ) ) {
      setSubStep( subStepTag );
    }
    setFileName( XMLHandler.getTagValue( stepnode, TRANSFORMATION_PATH ) );
    setBatchSize( XMLHandler.getTagValue( stepnode, BATCH_SIZE ) );
    setBatchDuration( XMLHandler.getTagValue( stepnode, BATCH_DURATION ) );
    setDirectBootstrapServers( XMLHandler.getTagValue( stepnode, DIRECT_BOOTSTRAP_SERVERS ) );
    List<Node> ofNode = XMLHandler.getNodes( stepnode, OUTPUT_FIELD_TAG_NAME );

    ofNode.forEach( node -> {
      String displayName = XMLHandler.getNodeValue( node );
      String kafkaName = XMLHandler.getTagAttribute( node, KAFKA_NAME_ATTRIBUTE );
      String type = XMLHandler.getTagAttribute( node, TYPE_ATTRIBUTE );
      KafkaConsumerField field = new KafkaConsumerField(
        KafkaConsumerField.Name.valueOf( kafkaName.toUpperCase() ),
        displayName,
        KafkaConsumerField.Type.valueOf( type ) );

      setField( field );
    } );

    config = new LinkedHashMap<>();

    Optional.ofNullable( XMLHandler.getSubNode( stepnode, ADVANCED_CONFIG ) ).map( Node::getChildNodes )
        .ifPresent( nodes -> IntStream.range( 0, nodes.getLength() ).mapToObj( nodes::item )
            .filter( node -> node.getNodeType() == Node.ELEMENT_NODE )
            .forEach( node -> {
              if ( CONFIG_OPTION.equals( node.getNodeName() ) ) {
                config.put( node.getAttributes().getNamedItem( OPTION_PROPERTY ).getTextContent(),
                            node.getAttributes().getNamedItem( OPTION_VALUE ).getTextContent() );
              } else {
                config.put( node.getNodeName(), node.getTextContent() );
              }
            } ) );
  }

  protected void setField( KafkaConsumerField field ) {
    field.getKafkaName().setFieldOnMeta( this, field );
  }

  public void setDefault() {
    batchSize = "1000";
    batchDuration = "1000";
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    int topicCount = rep.countNrStepAttributes( id_step, TOPIC );
    for ( int i = 0; i < topicCount; i++ ) {
      addTopic( rep.getStepAttributeString( id_step, i, TOPIC ) );
    }

    setConsumerGroup( rep.getStepAttributeString( id_step, CONSUMER_GROUP ) );
    setTransformationPath( rep.getStepAttributeString( id_step, TRANSFORMATION_PATH ) );
    setSubStep( rep.getStepAttributeString( id_step, SUB_STEP ) );
    setFileName( rep.getStepAttributeString( id_step, TRANSFORMATION_PATH ) );
    setBatchSize( rep.getStepAttributeString( id_step, BATCH_SIZE ) );
    setBatchDuration( rep.getStepAttributeString( id_step, BATCH_DURATION ) );
    setDirectBootstrapServers( rep.getStepAttributeString( id_step, DIRECT_BOOTSTRAP_SERVERS ) );

    for ( KafkaConsumerField.Name name : KafkaConsumerField.Name.values() ) {
      String prefix = OUTPUT_FIELD_TAG_NAME + "_" + name;
      String value = rep.getStepAttributeString( id_step, prefix );
      String type = rep.getStepAttributeString( id_step, prefix + "_" + TYPE_ATTRIBUTE );
      if ( value != null ) {
        setField( new KafkaConsumerField( name, value, KafkaConsumerField.Type.valueOf( type ) ) );
      }
    }

    config = new LinkedHashMap<>();

    for ( int i = 0; i < rep.getStepAttributeInteger( id_step, ADVANCED_CONFIG + "_COUNT" ); i++ ) {
      config.put( rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_NAME" ),
          rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_VALUE" ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId )
    throws KettleException {

    int i = 0;
    for ( String topic : topics ) {
      rep.saveStepAttribute( transId, stepId, i++, TOPIC, topic );
    }

    rep.saveStepAttribute( transId, stepId, CONSUMER_GROUP, consumerGroup );
    rep.saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, transformationPath );
    rep.saveStepAttribute( transId, stepId, SUB_STEP, getSubStep() );
    rep.saveStepAttribute( transId, stepId, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transId, stepId, BATCH_DURATION, batchDuration );
    rep.saveStepAttribute( transId, stepId, DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers );

    List<KafkaConsumerField> fields = getFieldDefinitions();
    for ( KafkaConsumerField field : fields ) {
      String prefix = OUTPUT_FIELD_TAG_NAME + "_" + field.getKafkaName().toString();
      rep.saveStepAttribute( transId, stepId, prefix, field.getOutputName() );
      rep.saveStepAttribute( transId, stepId, prefix + "_" + TYPE_ATTRIBUTE, field.getOutputType().toString() );
    }

    rep.saveStepAttribute( transId, stepId, ADVANCED_CONFIG + "_COUNT", getConfig().size() );

    i = 0;
    for ( String propName : getConfig().keySet() ) {
      rep.saveStepAttribute( transId, stepId, i, ADVANCED_CONFIG + "_NAME", propName );
      rep.saveStepAttribute( transId, stepId, i++, ADVANCED_CONFIG + "_VALUE", getConfig().get( propName ) );
    }
  }

  @Override
  public RowMeta getRowMeta( String origin, VariableSpace space ) throws KettleStepException {
    RowMeta rowMeta = new RowMeta();
    putFieldOnRowMeta( getKeyField(), rowMeta, origin, space );
    putFieldOnRowMeta( getMessageField(), rowMeta, origin, space );
    putFieldOnRowMeta( getTopicField(), rowMeta, origin, space );
    putFieldOnRowMeta( getPartitionField(), rowMeta, origin, space );
    putFieldOnRowMeta( getOffsetField(), rowMeta, origin, space );
    putFieldOnRowMeta( getTimestampField(), rowMeta, origin, space );
    return rowMeta;
  }

  private void putFieldOnRowMeta( KafkaConsumerField field, RowMetaInterface rowMeta,
                                  String origin, VariableSpace space ) throws KettleStepException {
    if ( field != null && !Utils.isEmpty( field.getOutputName() ) ) {
      try {
        String value = space.environmentSubstitute( field.getOutputName() );
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( value,
          field.getOutputType().getValueMetaInterfaceType() );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( BaseMessages.getString(
          PKG,
          "KafkaConsumerInputMeta.UnableToCreateValueType",
          field
        ), e );
      }
    }
  }


  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new KafkaConsumerInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new KafkaConsumerInputData();
  }

  public void setTopics( ArrayList<String> topics ) {
    this.topics = topics;
  }

  public void addTopic( String topic ) {
    this.topics.add( topic );
  }

  public void setConsumerGroup( String consumerGroup ) {
    this.consumerGroup = consumerGroup;
  }

  public List<String> getTopics() {
    return topics;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public KafkaConsumerField getKeyField() {
    return keyField;
  }

  public KafkaConsumerField getMessageField() {
    return messageField;
  }

  public KafkaConsumerField getTopicField() {
    return topicField;
  }

  public KafkaConsumerField getOffsetField() {
    return offsetField;
  }

  public KafkaConsumerField getPartitionField() {
    return partitionField;
  }

  public KafkaConsumerField getTimestampField() {
    return timestampField;
  }

  public String getDirectBootstrapServers() {
    return directBootstrapServers;
  }

  public void setKeyField( KafkaConsumerField keyField ) {
    this.keyField = keyField;
  }

  public void setMessageField( KafkaConsumerField messageField ) {
    this.messageField = messageField;
  }

  public void setTopicField( KafkaConsumerField topicField ) {
    this.topicField = topicField;
  }

  public void setOffsetField( KafkaConsumerField offsetField ) {
    this.offsetField = offsetField;
  }

  public void setPartitionField( KafkaConsumerField partitionField ) {
    this.partitionField = partitionField;
  }

  public void setTimestampField( KafkaConsumerField timestampField ) {
    this.timestampField = timestampField;
  }

  public void setDirectBootstrapServers( final String directBootstrapServers ) {
    this.directBootstrapServers = directBootstrapServers;
  }

  @Override public String getXML() {
    StringBuilder retval = new StringBuilder();

    getTopics().forEach( topic ->
      retval.append( "    " ).append( XMLHandler.addTagValue( TOPIC, topic ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( CONSUMER_GROUP, consumerGroup ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( TRANSFORMATION_PATH, transformationPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( SUB_STEP, getSubStep() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_DURATION, batchDuration ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers ) );

    getFieldDefinitions().forEach( field ->
      retval.append( "    " ).append(
        XMLHandler.addTagValue( OUTPUT_FIELD_TAG_NAME, field.getOutputName(), true,
          KAFKA_NAME_ATTRIBUTE, field.getKafkaName().toString(),
          TYPE_ATTRIBUTE, field.getOutputType().toString() ) ) );

    retval.append( "    " ).append( XMLHandler.openTag( ADVANCED_CONFIG ) ).append( Const.CR );
    getConfig().forEach( ( key, value ) -> retval.append( "        " )
        .append( XMLHandler.addTagValue( CONFIG_OPTION, "", true,
                              OPTION_PROPERTY, (String) key, OPTION_VALUE, (String) value ) ) );
    retval.append( "    " ).append( XMLHandler.closeTag( ADVANCED_CONFIG ) ).append( Const.CR );

    return retval.toString();
  }

  public List<KafkaConsumerField> getFieldDefinitions() {
    return Lists.newArrayList(
      getKeyField(),
      getMessageField(),
      getTopicField(),
      getPartitionField(),
      getOffsetField(),
      getTimestampField() );
  }

  public void setConfig( Map<String, String> config ) {
    this.config = config;
  }

  public Map<String, String> getConfig() {
    applyInjectedProperties();
    return config;
  }

  protected void applyInjectedProperties() {
    if ( injectedConfigNames != null || injectedConfigValues != null ) {
      Preconditions.checkState( injectedConfigNames != null, "Options names were not injected" );
      Preconditions.checkState( injectedConfigValues != null, "Options values were not injected" );
      Preconditions.checkState( injectedConfigNames.size() == injectedConfigValues.size(),
          "Injected different number of options names and value" );

      setConfig( IntStream.range( 0, injectedConfigNames.size() ).boxed().collect( Collectors
          .toMap( injectedConfigNames::get, injectedConfigValues::get, ( v1, v2 ) -> v1,
              LinkedHashMap::new ) ) );

      injectedConfigNames = null;
      injectedConfigValues = null;
    }
  }


  @Override public KafkaConsumerInputMeta copyObject() {
    KafkaConsumerInputMeta newClone = (KafkaConsumerInputMeta) this.clone();
    newClone.topics = new ArrayList<>( this.topics );
    newClone.keyField = new KafkaConsumerField( this.keyField );
    newClone.messageField = new KafkaConsumerField( this.messageField );
    if ( null != this.injectedConfigNames ) {
      newClone.injectedConfigNames = new ArrayList<>( this.injectedConfigNames );
    }
    if ( null != this.injectedConfigValues ) {
      newClone.injectedConfigValues = new ArrayList<>( this.injectedConfigValues );
    }
    newClone.config = new LinkedHashMap<>( this.config );
    newClone.topicField = new KafkaConsumerField( this.topicField );
    newClone.offsetField = new KafkaConsumerField( this.offsetField );
    newClone.partitionField = new KafkaConsumerField( this.partitionField );
    newClone.timestampField = new KafkaConsumerField( this.timestampField );
    return newClone;
  }
}

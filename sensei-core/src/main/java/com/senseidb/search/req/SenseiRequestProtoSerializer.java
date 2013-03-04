package com.senseidb.search.req;


import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linkedin.norbert.network.Serializer;
import com.sensei.search.req.protobuf.SenseiProtos;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.functions.*;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;

import java.nio.charset.Charset;
import java.util.*;


public class SenseiRequestProtoSerializer implements Serializer<SenseiRequest, SenseiResult> {
    @Override
    public String requestName() {
        return "SenseiRequestV2";
    }

    @Override
    public String responseName() {
        return "SenseiResponseV2";
    }

    @Override
    public SenseiRequest requestFromBytes(byte[] bytes) {
        SenseiProtos.SenseiProtoRequest senseiProtoRequest = null;
        try {
            senseiProtoRequest = SenseiProtos.SenseiProtoRequest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Could not parse");
        }

        SenseiRequest senseiRequest = new SenseiRequest();

        for (SenseiProtos.BrowseSelection selection : senseiProtoRequest.getSelectionList()) {
            senseiRequest.addSelection(convertBrowseSelection(selection));
        }

        for (SenseiProtos.SortField sortField : senseiProtoRequest.getSortSpecList()) {
            senseiRequest.addSortField(convertSortField(sortField));
        }

        senseiRequest.setFacetSpecs(convertFacetSpecs(senseiProtoRequest.getFacetSpecList()));

        senseiRequest.setQuery(new SenseiQuery(senseiProtoRequest.getSenseiQuery().getBytes(Charset.forName("UTF-8"))));
        senseiRequest.setOffset(senseiProtoRequest.getOffset());
        senseiRequest.setCount(senseiProtoRequest.getCount());
        senseiRequest.setFetchStoredFields(senseiProtoRequest.getFetchStoredFields());
        senseiRequest.setFetchStoredValue(senseiProtoRequest.getFetchStoredValue());
        senseiRequest.setFacetHandlerInitParamMap(convertFacetParams(senseiProtoRequest.getFacetHandlerParam()));
        senseiRequest.setPartitions(new HashSet<Integer>(senseiProtoRequest.getPartitionsList()));
        senseiRequest.setShowExplanation(senseiProtoRequest.getExplain());
        senseiRequest.setRouteParam(senseiProtoRequest.getRouteParam());
        senseiRequest.setGroupBy(senseiProtoRequest.getGroupByList().toArray(new String[]{}));
        senseiRequest.setDistinct(senseiProtoRequest.getDistinctList().toArray(new String[]{}));
        senseiRequest.setMaxPerGroup(senseiProtoRequest.getMaxPerGroup());
        senseiRequest.setTermVectorsToFetch(new HashSet<String>(senseiProtoRequest.getTermVectorsToFetchList()));
        senseiRequest.setSelectList(senseiProtoRequest.getSelectListList());
        senseiRequest.setMapReduceFunction(convertMapReduce(senseiProtoRequest.getMapReduce()));

        return senseiRequest;
    }

    @Override
    public byte[] requestToBytes(SenseiRequest request) {
        SenseiProtos.SenseiProtoRequest.Builder builder = SenseiProtos.SenseiProtoRequest.newBuilder();

        if (request.getSelections() != null) {
            for (BrowseSelection browseSelection : request.getSelections()) {
                builder.addSelection(convertBrowseSelection(browseSelection));
            }
        }

        for (SortField sortField : request.getSort()) {
            builder.addSortSpec(convertSortField(sortField));
        }

        if (request.getFacetSpecs() != null) {
            for (Map.Entry<String, FacetSpec> facetSpec : request.getFacetSpecs().entrySet()) {
                builder.addFacetSpec(convertFacetSpec(facetSpec.getKey(), facetSpec.getValue()));
            }
        }

        if (request.getQuery() != null) builder.setSenseiQuery(request.getQuery().toString());

        builder.setOffset(request.getOffset()).setCount(request.getCount()).setFetchStoredFields(request.isFetchStoredFields()).setFetchStoredValue(
                request.isFetchStoredValue()).setFacetHandlerParam(convertFacetParams(request.getFacetHandlerInitParamMap())).addAllPartitions(
                request.getPartitions()).setExplain(request.isShowExplanation()).setRouteParam(request.getRouteParam()).addAllGroupBy(
                request.getGroupBy() == null ? Collections.<String>emptyList() : Arrays.asList(request.getGroupBy())).addAllDistinct(
                request.getDistinct() == null ? Collections.<String>emptyList() : Arrays.asList(request.getDistinct())).setMaxPerGroup(
                request.getMaxPerGroup()).addAllTermVectorsToFetch(request.getTermVectorsToFetch() == null ? Collections.<String>emptyList() : request.getTermVectorsToFetch()).addAllSelectList(
                request.getSelectList() == null ? Collections.<String>emptyList() : request.getSelectList());

        if (request.getMapReduceFunction() != null) {
            builder.setMapReduce(convertMapReduce(request.getMapReduceFunction()));
        }

        SenseiProtos.SenseiProtoRequest senseiProtoRequest = builder.build();
        return senseiProtoRequest.toByteArray();
    }

    @Override
    public SenseiResult responseFromBytes(byte[] bytes) {
        return null;
    }

    private SenseiProtos.JavaPrimitives getPrimitiveType(Object[] array) {
        if (array instanceof Integer[]) {
            return SenseiProtos.JavaPrimitives.INT;
        } else if (array instanceof Long[]) {
            return SenseiProtos.JavaPrimitives.LONG;
        } else if (array instanceof Byte[]) {
            return SenseiProtos.JavaPrimitives.BYTE;
        } else if (array instanceof Character[]) {
            return SenseiProtos.JavaPrimitives.CHAR;
        } else if (array instanceof Float[]) {
            return SenseiProtos.JavaPrimitives.FLOAT;
        } else if (array instanceof Double[]) {
            return SenseiProtos.JavaPrimitives.DOUBLE;
        } else if (array instanceof Integer[]) {
            return SenseiProtos.JavaPrimitives.INT;
        } else if (array instanceof Short[]) {
            return SenseiProtos.JavaPrimitives.SHORT;
        } else if (array instanceof String[]) {
            return SenseiProtos.JavaPrimitives.STRING;
        } else {
            throw new UnsupportedOperationException("Unsupport raw data type " + array.getClass());
        }
    }

    private Object[] convert(List<String> values, SenseiProtos.JavaPrimitives type) {
        switch (type) {
            case BOOLEAN: {
                Boolean[] result = new Boolean[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Boolean.parseBoolean(values.get(i));
                return result;
            }
            case INT: {
                Integer[] result = new Integer[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Integer.parseInt(values.get(i));
                return result;
            }
            case LONG: {
                Long[] result = new Long[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Long.parseLong(values.get(i));
                return result;
            }
            case FLOAT: {
                Float[] result = new Float[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Float.parseFloat(values.get(i));
                return result;
            }
            case DOUBLE: {
                Double[] result = new Double[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Double.parseDouble(values.get(i));
                return result;
            }
            case BYTE: {
                Byte[] result = new Byte[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Byte.parseByte(values.get(i));
                return result;
            }
            case SHORT: {
                Short[] result = new Short[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = Short.parseShort(values.get(i));
                return result;
            }
            case STRING: {
                String[] result = new String[values.size()];
                for (int i = 0; i < result.length; i++) result[i] = values.get(i);
                return result;
            }
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private SenseiProtos.Explanation convertExplanation(Explanation explanation) {
        SenseiProtos.Explanation.Builder builder = SenseiProtos.Explanation.newBuilder();

        builder.setDescription(explanation.getDescription());
        builder.setValue(explanation.getValue());
        if (explanation.getDetails() != null && explanation.getDetails().length > 0) {
            for (int i = 0; i < explanation.getDetails().length; i++) {
                builder.addDetails(convertExplanation(explanation.getDetails()[i]));
            }
        }

        return builder.build();
    }

    private Explanation convertExplanation(SenseiProtos.Explanation protoExplanation) {
        Explanation explanation = new Explanation(protoExplanation.getValue(), protoExplanation.getDescription());
        if (protoExplanation.getDetailsCount() > 0) {
            for (SenseiProtos.Explanation detail : protoExplanation.getDetailsList()) {
                explanation.addDetail(convertExplanation(detail));
            }
        }
        return explanation;
    }

    private SenseiProtos.SenseiHit convert(SenseiHit senseiHit) {
        SenseiProtos.SenseiHit.Builder builder = SenseiProtos.SenseiHit.newBuilder();
        builder.setScore(senseiHit.getScore());
        builder.setDocId(senseiHit.getDocid());

        for (Map.Entry<String, String[]> fieldValueTuple : senseiHit.getFieldValues().entrySet()) {
            SenseiProtos.StringParams.Builder stringParamBuilder = SenseiProtos.StringParams.newBuilder();
            stringParamBuilder.setKey(fieldValueTuple.getKey());
            if (fieldValueTuple.getValue() != null)
                stringParamBuilder.addAllValue(Arrays.asList(fieldValueTuple.getValue()));

            builder.addFieldValues(stringParamBuilder);

            Object[] rawFields = senseiHit.getRawFields(fieldValueTuple.getKey());
            builder.addFieldType(getPrimitiveType(rawFields));
        }

        builder.setGroupPosition(senseiHit.getGroupPosition());
        builder.setGroupField(senseiHit.getGroupField());
        builder.setGroupValue(senseiHit.getGroupValue());
        builder.setGroupHitCount(senseiHit.getGroupHitsCount());

        if (senseiHit.getGroupHits() != null && senseiHit.getGroupHits().length > 0) {
            for (BrowseHit groupHit : senseiHit.getGroupHits()) {
                if (groupHit instanceof SenseiHit) {
                    builder.addGroupHits(convert((SenseiHit) groupHit));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        if (senseiHit.getExplanation() != null) {
            builder.setExplanation(convertExplanation(senseiHit.getExplanation()));
        }

        builder.setUid(senseiHit.getUID());
        if (senseiHit.getSrcData() != null) {
            builder.setSrcData(senseiHit.getSrcData());
        }

        builder.setStoredValue(ByteString.copyFrom(senseiHit.getStoredValue()));
        return builder.build();
    }

    private SenseiHit convert(SenseiProtos.SenseiHit protoSenseiHit) {
        SenseiHit senseiHit = new SenseiHit();
        if (protoSenseiHit.hasScore()) {
            senseiHit.setScore(protoSenseiHit.getScore());
        }

        if (protoSenseiHit.hasDocId()) {
            senseiHit.setDocid(protoSenseiHit.getDocId());
        }

        int fieldValuesCount = protoSenseiHit.getFieldValuesCount();
        if (fieldValuesCount > 0) {
            Map<String, String[]> fieldValues = new HashMap<String, String[]>();
            Map<String, Object[]> rawFieldValues = new HashMap<String, Object[]>();
            for (int i = 0; i < fieldValuesCount; i++) {
                SenseiProtos.StringParams protoFieldValues = protoSenseiHit.getFieldValues(i);
                SenseiProtos.JavaPrimitives fieldType = protoSenseiHit.getFieldType(i);

                fieldValues.put(protoFieldValues.getKey(), protoFieldValues.getValueList().toArray(new String[]{}));
                rawFieldValues.put(protoFieldValues.getKey(), convert(protoFieldValues.getValueList(), fieldType));
            }
            senseiHit.setFieldValues(fieldValues);
            senseiHit.setRawFieldValues(rawFieldValues);
        }

        if (protoSenseiHit.hasGroupPosition()) {
            senseiHit.setGroupPosition(protoSenseiHit.getGroupPosition());
        }

        if (protoSenseiHit.hasGroupField()) {
            senseiHit.setGroupField(protoSenseiHit.getGroupField());
        }

        if (protoSenseiHit.hasGroupValue()) {
            senseiHit.setGroupValue(protoSenseiHit.getGroupValue());
        }

        if(protoSenseiHit.hasGroupHitCount()) {
            senseiHit.setGroupHitsCount(protoSenseiHit.getGroupHitCount());
        }

        if(protoSenseiHit.getGroupHitsList() != null && protoSenseiHit.getGroupHitsList().size() > 0) {
            SenseiHit[] groupHits = new  SenseiHit[protoSenseiHit.getGroupHitsList().size()];
            int i = 0;
            for(SenseiProtos.SenseiHit hit : protoSenseiHit.getGroupHitsList()) {
                groupHits[i++] = convert(hit);
            }
            senseiHit.setGroupHits(groupHits);
        }

        if(protoSenseiHit.hasExplanation()) {
            senseiHit.setExplanation(convertExplanation(protoSenseiHit.getExplanation()));
        }

        if(protoSenseiHit.hasUid()) {
            senseiHit.setUID(protoSenseiHit.getUid());
        }

        if(protoSenseiHit.hasSrcData()) {
            senseiHit.setSrcData(protoSenseiHit.getSrcData());
        }

        if(protoSenseiHit.hasStoredValue()) {
            senseiHit.setStoredValue(protoSenseiHit.getStoredValue().toByteArray());
        }

        return senseiHit;
    }

    @Override
    public byte[] responseToBytes(SenseiResult senseiResult) {
        SenseiProtos.SenseiProtoResult.Builder builder = SenseiProtos.SenseiProtoResult.newBuilder();
        builder.setParsedQuery(senseiResult.getParsedQuery());

        // TODO: Sensei errors
        if (senseiResult.getSenseiHits() != null && senseiResult.getSenseiHits().length > 0) {
            for (SenseiHit senseiHit : senseiResult.getSenseiHits()) {
                builder.addHit(convert(senseiHit));
            }
        }

        return builder.build().toByteArray();
    }

    private SenseiProtos.BooleanOperator convertOperator(BrowseSelection.ValueOperation valueOperation) {
        switch (valueOperation) {
            case ValueOperationAnd:
                return SenseiProtos.BooleanOperator.AND;
            case ValueOperationOr:
                return SenseiProtos.BooleanOperator.OR;
            default:
                throw new IllegalArgumentException();
        }
    }

    private BrowseSelection.ValueOperation convertOperator(SenseiProtos.BooleanOperator valueOperation) {
        switch (valueOperation) {
            case AND:
                return BrowseSelection.ValueOperation.ValueOperationAnd;
            case OR:
                return BrowseSelection.ValueOperation.ValueOperationOr;
            default:
                throw new IllegalArgumentException();
        }
    }

    private SenseiProtos.StringProperties convertProperties(Map<?, ?> map) {
        SenseiProtos.StringProperties.Builder stringPropertiesBuilder = SenseiProtos.StringProperties.newBuilder();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            stringPropertiesBuilder.addKey(key);
            stringPropertiesBuilder.addValue(value);
        }
        return stringPropertiesBuilder.build();
    }

    private Map<String, String> convertProperties(SenseiProtos.StringProperties stringProperties) {
        Map<String, String> results = new HashMap<String, String>();
        for (int i = 0; i < stringProperties.getKeyCount(); i++) {
            results.put(stringProperties.getKey(i), stringProperties.getValue(i));
        }
        return results;
    }

    private SenseiProtos.BrowseSelection convertBrowseSelection(BrowseSelection browseSelection) {
        BrowseSelection.ValueOperation selectionOperation = browseSelection.getSelectionOperation();

        return SenseiProtos.BrowseSelection.newBuilder().setOperator(convertOperator(selectionOperation)).setFieldName(
                browseSelection.getFieldName()).addAllValue(browseSelection.getValues() == null ? Collections.<String>emptyList() : Arrays.asList(
                browseSelection.getValues())).addAllNotValue(browseSelection.getNotValues() == null ? Collections.<String>emptyList() : Arrays.asList(
                browseSelection.getNotValues())).setProperties(convertProperties(browseSelection.getSelectionProperties())).build();
    }

    private BrowseSelection convertBrowseSelection(SenseiProtos.BrowseSelection protoBrowseSelection) {
        BrowseSelection browseSelection = new BrowseSelection(protoBrowseSelection.getFieldName());
        browseSelection.setSelectionOperation(convertOperator(protoBrowseSelection.getOperator()));
        browseSelection.setValues(protoBrowseSelection.getValueList().toArray(new String[]{}));
        browseSelection.setNotValues(protoBrowseSelection.getNotValueList().toArray(new String[]{}));
        browseSelection.setSelectionProperties(convertProperties(protoBrowseSelection.getProperties()));

        return browseSelection;
    }

    private SenseiProtos.Locale convertLocale(Locale locale) {
        return SenseiProtos.Locale.newBuilder().setCountry(locale.getCountry()).setLanguage(locale.getLanguage()).setVariant(
                locale.getVariant()).build();
    }

    private Locale convertLocale(SenseiProtos.Locale locale) {
        return new Locale(locale.getCountry(), locale.getLanguage(), locale.getVariant());
    }

    private SenseiProtos.SortField convertSortField(SortField sortField) {
        return SenseiProtos.SortField.newBuilder().setField(sortField.getField()).setType(sortField.getType()).setLocale(
                convertLocale(sortField.getLocale())).setReverse(sortField.getReverse()).build();
    }

    private SortField convertSortField(SenseiProtos.SortField protoSortField) {
        SortField sortField = new SortField(protoSortField.getField(),
                protoSortField.getType(),
                protoSortField.getReverse());
        return sortField;
    }

    private SenseiProtos.SortOrder convertSortOrder(FacetSpec.FacetSortSpec facetSortSpec) {
        switch (facetSortSpec) {
            case OrderValueAsc:
                return SenseiProtos.SortOrder.ASCENDING;
            case OrderHitsDesc:
                return SenseiProtos.SortOrder.DESCENDING;
            case OrderByCustom:
                return SenseiProtos.SortOrder.CUSTOM;
            default:
                throw new IllegalArgumentException();
        }
    }

    private FacetSpec.FacetSortSpec convertSortOrder(SenseiProtos.SortOrder sortOrder) {
        switch (sortOrder) {
            case ASCENDING:
                return FacetSpec.FacetSortSpec.OrderValueAsc;
            case DESCENDING:
                return FacetSpec.FacetSortSpec.OrderHitsDesc;
            case CUSTOM:
                return FacetSpec.FacetSortSpec.OrderByCustom;
            default:
                throw new IllegalArgumentException();
        }
    }

    private SenseiProtos.FacetSpec convertFacetSpec(String field, FacetSpec facetSpec) {
        return SenseiProtos.FacetSpec.newBuilder().setOrderBy(convertSortOrder(facetSpec.getOrderBy())).setMax(facetSpec.getMaxCount()).setExpandSelection(
                facetSpec.isExpandSelection()).setMinCount(facetSpec.getMinHitCount()).setProperties(convertProperties(facetSpec.getProperties())).setField(
                field).build();
    }

    private Map<String, FacetSpec> convertFacetSpecs(Iterable<SenseiProtos.FacetSpec> facetSpecs) {
        Map<String, FacetSpec> result = new HashMap<String, FacetSpec>();
        for (SenseiProtos.FacetSpec protoFacetSpec : facetSpecs) {
            FacetSpec facetSpec = new FacetSpec();
            facetSpec.setOrderBy(convertSortOrder(protoFacetSpec.getOrderBy()));
            facetSpec.setMaxCount(protoFacetSpec.getMax());
            facetSpec.setExpandSelection(protoFacetSpec.getExpandSelection());
            facetSpec.setMinHitCount(protoFacetSpec.getMinCount());
            facetSpec.setProperties(convertProperties(protoFacetSpec.getProperties()));
            result.put(protoFacetSpec.getField(), facetSpec);
        }
        return result;
    }

    private SenseiProtos.SenseiMapReduceFunction convertMapReduce(SenseiMapReduce mapReduce) {

        if (mapReduce instanceof AvgMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.AVG;
        } else if (mapReduce instanceof CountGroupByMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.COUNT_GROUP_BY;
        } else if (mapReduce instanceof DistinctCountMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.DISTINCT_COUNT;
        } else if (mapReduce instanceof DistinctUIDCount) {
            return SenseiProtos.SenseiMapReduceFunction.DISTINCT_UID;
        } else if (mapReduce instanceof FacetCountsMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.FACET_COUNTS;
        } else if (mapReduce instanceof HashSetDistinctCountMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.HASH_SET_DISTINCT_COUNT;
        } else if (mapReduce instanceof MaxMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.MAX;
        } else if (mapReduce instanceof MinMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.MIN;
        } else if (mapReduce instanceof SumMapReduce) {
            return SenseiProtos.SenseiMapReduceFunction.SUM;
        } else {
            throw new IllegalArgumentException("Unknown map reduce function. Cannot serialize to protobuf " + mapReduce.getClass());
        }
    }

    private SenseiMapReduce convertMapReduce(SenseiProtos.SenseiMapReduceFunction mapReduce) {
        switch (mapReduce) {
            case AVG:
                return new AvgMapReduce();
            case COUNT_GROUP_BY:
                return new CountGroupByMapReduce();
            case DISTINCT_COUNT:
                return new DistinctCountMapReduce();
            case DISTINCT_UID:
                return new DistinctUIDCount();
            case FACET_COUNTS:
                return new FacetCountsMapReduce();
            case HASH_SET_DISTINCT_COUNT:
                return new HashSetDistinctCountMapReduce();
            case MAX:
                return new MaxMapReduce();
            case MIN:
                return new MinMapReduce();
            case SUM:
                return new SumMapReduce();
            default:
                throw new IllegalArgumentException("Cannot translate " + mapReduce + " from protobuf");
        }
    }

    private SenseiProtos.FacetHandlerInitializerParams convertFacetParams(Map<String, FacetHandlerInitializerParam> facetHandlerInitializerParamMap) {
        SenseiProtos.FacetHandlerInitializerParams.Builder builder = SenseiProtos.FacetHandlerInitializerParams.newBuilder();
        for (Map.Entry<String, FacetHandlerInitializerParam> entry : facetHandlerInitializerParamMap.entrySet()) {
            builder.addKey(entry.getKey());
            builder.addValue(convertFacetParam(entry.getValue()));
        }
        return builder.build();
    }

    private Map<String, FacetHandlerInitializerParam> convertFacetParams(SenseiProtos.FacetHandlerInitializerParams protoFacetHandlerInitializerParams) {
        Map<String, FacetHandlerInitializerParam> result = new HashMap<String, FacetHandlerInitializerParam>();
        for (int i = 0; i < protoFacetHandlerInitializerParams.getKeyCount(); i++) {
            result.put(protoFacetHandlerInitializerParams.getKey(i),
                    convertFacetParam(protoFacetHandlerInitializerParams.getValue(i)));
        }
        return result;
    }

    private SenseiProtos.FacetHandlerInitializerParam convertFacetParam(FacetHandlerInitializerParam facetHandlerInitializerParam) {
        SenseiProtos.FacetHandlerInitializerParam.Builder protoParams = SenseiProtos.FacetHandlerInitializerParam.newBuilder();

        for (String paramName : facetHandlerInitializerParam.getBooleanParamNames()) {
            SenseiProtos.BooleanParams.Builder paramBuilder = SenseiProtos.BooleanParams.newBuilder();
            paramBuilder.setKey(paramName);
            boolean[] param = facetHandlerInitializerParam.getBooleanParam(paramName);
            for (int i = 0; i < param.length; i++) {
                paramBuilder.addValue(param[i]);
            }
            protoParams.addBooleanParam(paramBuilder.build());
        }

        for (String paramName : facetHandlerInitializerParam.getIntParamNames()) {
            SenseiProtos.IntParams.Builder paramBuilder = SenseiProtos.IntParams.newBuilder();
            paramBuilder.setKey(paramName);
            int[] param = facetHandlerInitializerParam.getIntParam(paramName);
            for (int i = 0; i < param.length; i++) {
                paramBuilder.addValue(param[i]);
            }
            protoParams.addIntParam(paramBuilder.build());
        }

        for (String paramName : facetHandlerInitializerParam.getLongParamNames()) {
            SenseiProtos.LongParams.Builder paramBuilder = SenseiProtos.LongParams.newBuilder();
            paramBuilder.setKey(paramName);
            long[] param = facetHandlerInitializerParam.getLongParam(paramName);
            for (int i = 0; i < param.length; i++) {
                paramBuilder.addValue(param[i]);
            }
            protoParams.addLongParam(paramBuilder.build());
        }

        for (String paramName : facetHandlerInitializerParam.getBooleanParamNames()) {
            SenseiProtos.StringParams.Builder paramBuilder = SenseiProtos.StringParams.newBuilder();
            paramBuilder.setKey(paramName);
            List<String> param = facetHandlerInitializerParam.getStringParam(paramName);
            for (int i = 0; i < param.size(); i++) {
                paramBuilder.addValue(param.get(i));
            }
            protoParams.addStringParam(paramBuilder.build());
        }

        for (String paramName : facetHandlerInitializerParam.getDoubleParamNames()) {
            SenseiProtos.DoubleParams.Builder paramBuilder = SenseiProtos.DoubleParams.newBuilder();
            paramBuilder.setKey(paramName);
            double[] param = facetHandlerInitializerParam.getDoubleParam(paramName);
            for (int i = 0; i < param.length; i++) {
                paramBuilder.addValue(param[i]);
            }
            protoParams.addDoubleParam(paramBuilder.build());
        }

        for (String paramName : facetHandlerInitializerParam.getByteArrayParamNames()) {
            SenseiProtos.ByteArrayParams.Builder paramBuilder = SenseiProtos.ByteArrayParams.newBuilder();
            paramBuilder.setKey(paramName);
            byte[] param = facetHandlerInitializerParam.getByteArrayParam(paramName);

            for (int i = 0; i < param.length; i++) {
                paramBuilder.setValue(ByteString.copyFrom(param));
            }
            protoParams.addByteParam(paramBuilder.build());
        }

        return protoParams.build();
    }

    private FacetHandlerInitializerParam convertFacetParam(SenseiProtos.FacetHandlerInitializerParam protoFacetHandlerInitializerParam) {
        DefaultFacetHandlerInitializerParam facetHandlerInitializerParam = new DefaultFacetHandlerInitializerParam();

        for (SenseiProtos.BooleanParams param : protoFacetHandlerInitializerParam.getBooleanParamList()) {
            boolean[] value = new boolean[param.getValueCount()];
            for (int i = 0; i < value.length; i++) {
                value[i] = param.getValue(i);
            }
            facetHandlerInitializerParam.putBooleanParam(param.getKey(), value);
        }

        for (SenseiProtos.IntParams param : protoFacetHandlerInitializerParam.getIntParamList()) {
            int[] value = new int[param.getValueCount()];
            for (int i = 0; i < value.length; i++) {
                value[i] = param.getValue(i);
            }
            facetHandlerInitializerParam.putIntParam(param.getKey(), value);
        }

        for (SenseiProtos.LongParams param : protoFacetHandlerInitializerParam.getLongParamList()) {
            long[] value = new long[param.getValueCount()];
            for (int i = 0; i < value.length; i++) {
                value[i] = param.getValue(i);
            }
            facetHandlerInitializerParam.putLongParam(param.getKey(), value);
        }

        for (SenseiProtos.StringParams param : protoFacetHandlerInitializerParam.getStringParamList()) {
            facetHandlerInitializerParam.putStringParam(param.getKey(), param.getValueList());
        }

        for (SenseiProtos.DoubleParams param : protoFacetHandlerInitializerParam.getDoubleParamList()) {
            double[] value = new double[param.getValueCount()];
            for (int i = 0; i < value.length; i++) {
                value[i] = param.getValue(i);
            }
            facetHandlerInitializerParam.putDoubleParam(param.getKey(), value);
        }

        for (SenseiProtos.ByteArrayParams param : protoFacetHandlerInitializerParam.getByteParamList()) {
            ByteString value = param.getValue();
            facetHandlerInitializerParam.putByteArrayParam(param.getKey(), value.toByteArray());
        }

        return facetHandlerInitializerParam;
    }
}

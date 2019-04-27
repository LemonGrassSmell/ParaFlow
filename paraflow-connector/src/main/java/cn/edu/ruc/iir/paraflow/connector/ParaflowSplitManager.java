/*
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
package cn.edu.ruc.iir.paraflow.connector;

import cn.edu.ruc.iir.paraflow.commons.ParaflowFiberPartitioner;
import cn.edu.ruc.iir.paraflow.commons.utils.BytesUtils;
import cn.edu.ruc.iir.paraflow.connector.exception.TableNotFoundException;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowColumnHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableHandle;
import cn.edu.ruc.iir.paraflow.connector.handle.ParaflowTableLayoutHandle;
import cn.edu.ruc.iir.paraflow.connector.impl.ParaflowMetaDataReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
//import java.util.stream.Collectors;

import static cn.edu.ruc.iir.paraflow.connector.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class ParaflowSplitManager
implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(ParaflowSplitManager.class.getName());
    private final ParaflowConnectorId connectorId;
    private final ParaflowMetaDataReader metaDataQuery;
    private final FSFactory fsFactory;

    @Inject
    public ParaflowSplitManager(
            ParaflowConnectorId connectorId,
            ParaflowMetaDataReader metaDataQuer,
            FSFactory fsFactory)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.metaDataQuery = requireNonNull(metaDataQuer, "metaServer is null");
        this.fsFactory = requireNonNull(fsFactory, "fsFactory is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session,
                                          ConnectorTableLayoutHandle layoutHandle,
                                          SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ParaflowTableLayoutHandle layout = checkType(layoutHandle, ParaflowTableLayoutHandle.class, "layoutHandle");
        Optional<ParaflowTableHandle> tableHandle =
                metaDataQuery.getTableHandle(connectorId.getConnectorId(),
                        layout.getSchemaTableName().getSchemaName(),
                        layout.getSchemaTableName().getTableName());
        if (!tableHandle.isPresent()) {
            throw new TableNotFoundException(layout.getSchemaTableName().toString());
        }
        String tablePath = tableHandle.get().getPath();
        String dbName = tableHandle.get().getSchemaName();
        String tblName = tableHandle.get().getTableName();
        String partitionerName = layout.getFiberPartitioner();
        Optional<TupleDomain<ColumnHandle>> predicatesOptional = layout.getPredicates();

        List<ConnectorSplit> splits = new ArrayList<>();
        List<Path> files;
        if (predicatesOptional.isPresent()) {
            TupleDomain<ColumnHandle> predicates = predicatesOptional.get();
            ColumnHandle fiberCol = layout.getFiberColumn();
            ColumnHandle timeCol = layout.getTimestampColumn();
            Map<String, Integer> sortColumns = new HashMap<>();
            sortColumns.put("lo_totalprice", 3);
            sortColumns.put("lo_quantity", 10);
            sortColumns.put("lo_extendedprice", 11);
            sortColumns.put("lo_discount", 12);
            sortColumns.put("lo_tax", 13);
            int sortColumnId = -1;
            Optional<Map<ColumnHandle, Domain>> domains = predicates.getDomains();
            if (!domains.isPresent()) {
                files = fsFactory.listFiles(new Path(tablePath));
            }
            else {
                int fiber = -1;
                int fiberId = -1;
//                int sortColumnId = -1;
                long timeLow = -1L;
                long timeHigh = -1L;
                Set<ColumnHandle> keys = domains.get().keySet();
                int keyscount = keys.size();
                if (domains.get().containsKey(fiberCol)) {
                    // parse fiber domains
                    Domain fiberDomain = domains.get().get(fiberCol);
                    ValueSet fiberValueSet = fiberDomain.getValues();
                    if (fiberValueSet instanceof SortedRangeSet) {
                        if (fiberValueSet.isSingleValue()) {
                            Object valueObj = fiberValueSet.getSingleValue();
                            if (valueObj instanceof Integer) {
                                fiber = (int) valueObj;
                            }
                            if (valueObj instanceof Long) {
                                fiber = ((Long) valueObj).intValue();
                            }
                            if (valueObj instanceof Slice) {
                                Slice fiberSlice = (Slice) fiberValueSet.getSingleValue();
                                fiber = Integer.parseInt(fiberSlice.toStringUtf8());
                            }
                            ParaflowFiberPartitioner partitioner = parsePartitioner(partitionerName);
                            if (partitioner != null) {
                                fiberId = partitioner.getFiberId(BytesUtils.toBytes(fiber)); // mod fiberNum
                            }
                        }
                    }
                }
                if (domains.get().containsKey(timeCol)) {
                    // parse time domains
                    Domain timeDomain = domains.get().get(timeCol);
                    ValueSet timeValueSet = timeDomain.getValues();
                    if (timeValueSet instanceof SortedRangeSet) {
                        Range range = ((SortedRangeSet) timeValueSet).getOrderedRanges().get(0);
                        Marker low = range.getLow();
                        Marker high = range.getHigh();
                        if (!low.isLowerUnbounded()) {
                            timeLow = (Long) low.getValue();
                        }
                        if (!high.isUpperUnbounded()) {
                            timeHigh = (Long) high.getValue();
                        }
                    }
                }
                for (ColumnHandle key : keys) {
                    if (sortColumns.containsKey(((ParaflowColumnHandle) key).getName())) {
                        sortColumnId = sortColumns.get(((ParaflowColumnHandle) key).getName());
                        break;
                    }
                }
                fiberId = fiber % 8;
                //假设所有的查询都是聚合查询,直接按照聚合查询处理,不再判断了
//                if (fiberId == -1 && timeLow == -1L && timeHigh == -1L && sortColumnId == -1) {
                    files = fsFactory.listFiles(new Path(tablePath));
//                }
//                else {
//                    files = metaDataQuery.filterBlocks(
//                            dbName,
//                            tblName,
//                            fiberId,
//                            timeLow,
//                            timeHigh,
//                            sortColumnId)
//                            .stream().map(Path::new).collect(Collectors.toList());
//                }
            }
        }
        else {
            files = fsFactory.listFiles(new Path(tablePath));
        }

        files.forEach(file -> splits.add(new ParaflowSplit(connectorId,
                        tableHandle.get().getSchemaTableName(),
                        file.toString(), 0, -1,
                        fsFactory.getBlockLocations(file, 0, Long.MAX_VALUE))));
//        splits.forEach(split -> logger.info(split.toString()));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private ParaflowFiberPartitioner parsePartitioner(String partitionerName)
    {
//        Class clazz = Class.forName(partitionerName);
//        Constructor c = clazz.getConstructor(int.class);
//        ParaflowFiberPartitioner partitioner = c.newInstance(partitionNum);
        try {
            Class clazz = ParaflowMetaDataReader.class.getClassLoader().loadClass(partitionerName);
            return (ParaflowFiberPartitioner) clazz.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }
}

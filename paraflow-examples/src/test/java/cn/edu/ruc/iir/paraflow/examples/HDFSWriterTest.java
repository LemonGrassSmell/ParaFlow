package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.collector.DataSource;
import cn.edu.ruc.iir.paraflow.commons.Message;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.examples.collector.TpchDataSource;
import cn.edu.ruc.iir.paraflow.examples.loader.TpchDataTransformer;
import cn.edu.ruc.iir.paraflow.loader.ParaflowSegment;
import cn.edu.ruc.iir.paraflow.loader.ParquetSegmentWriter;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CLERK;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.COMMIT_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CREATION;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.CUSTOMER_KEY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.DISCOUNT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.EXTENDED_PRICE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINEITEM_COMMENT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINEORDER_KEY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.LINE_NUMBER;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_COMMENT;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_PRIORITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.ORDER_STATUS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.QUANTITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.RECEIPT_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.RETURN_FLAG;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_DATE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_INSTRUCTIONS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_MODE;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.SHIP_PRIORITY;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.STATUS;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.TAX;
import static cn.edu.ruc.iir.paraflow.benchmark.model.LineOrderColumn.TOTAL_PRICE;
import static org.testng.Assert.assertEquals;

public class HDFSWriterTest
{
    private final MetaClient metaClient = new MetaClient("127.0.0.1", 10012);
    private String dbName = "test";
    private String tableName = "linkorder";
    @Test
    public void testDebug()
    {
        final Random random = new Random(System.nanoTime());
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        int parallelism = 1;
        int sf = 100;
        int part = 1;
        int partCount = 1;
        long minCustKey = 0;
        long maxCustKey = 10000;
//        StatusProto.ResponseStatus status1 = metaClient.createUser("alice1", "123456");
//        assertEquals(expect, status1);
        MetaProto.StringListType dbs = metaClient.listDatabases();
        boolean dbExisted = false;
        for (int i = 0; i < dbs.getStrCount(); i++) {
            if (dbs.getStr(i).equals(dbName)) {
                System.out.println(dbs.getStr(i));
                dbExisted = true;
                break;
            }
        }
        if(!dbExisted) {
            StatusProto.ResponseStatus status = metaClient.createDatabase(dbName,"alice1");
            assertEquals(expect, status);
        }
        MetaProto.StringListType tbls = metaClient.listTables(dbName);
        //if table not exist,create table
        boolean tblExisted = false;
        for (int i = 0; i < tbls.getStrCount(); i++) {
            if (tbls.getStr(i).equals(tableName)) {
                System.out.println(tbls.getStr(i));
                tblExisted = true;
                break;
            }
        }
        if (!tblExisted) {
            String[] names = {LINEORDER_KEY.getColumnName(),
                    CUSTOMER_KEY.getColumnName(),
                    ORDER_STATUS.getColumnName(),
                    TOTAL_PRICE.getColumnName(),
                    ORDER_DATE.getColumnName(),
                    ORDER_PRIORITY.getColumnName(),
                    CLERK.getColumnName(),
                    SHIP_PRIORITY.getColumnName(),
                    ORDER_COMMENT.getColumnName(),
                    LINE_NUMBER.getColumnName(),
                    QUANTITY.getColumnName(),
                    EXTENDED_PRICE.getColumnName(),
                    DISCOUNT.getColumnName(),
                    TAX.getColumnName(),
                    RETURN_FLAG.getColumnName(),
                    STATUS.getColumnName(),
                    SHIP_DATE.getColumnName(),
                    COMMIT_DATE.getColumnName(),
                    RECEIPT_DATE.getColumnName(),
                    SHIP_INSTRUCTIONS.getColumnName(),
                    SHIP_MODE.getColumnName(),
                    LINEITEM_COMMENT.getColumnName(),
                    CREATION.getColumnName()
            };
            String[] types = {"bigint",
                    "bigint",
                    "varchar(1)",
                    "double",
                    "integer",
                    "varchar(15)",
                    "varchar(15)",
                    "integer",
                    "varchar(79)",
                    "integer",
                    "double",
                    "double",
                    "double",
                    "double",
                    "varchar(1)",
                    "varchar(1)",
                    "integer",
                    "integer",
                    "integer",
                    "varchar(25)",
                    "varchar(10)",
                    "varchar(44)",
                    "bigint"
            };
            metaClient.createTable(dbName, tableName, "parquet", 0,
                    "cn.edu.ruc.iir.paraflow.examples.collector.BasicParaflowFiberPartitioner", 22,
                    Arrays.asList(names), Arrays.asList(types));
        }
        DataSource dataSource = new TpchDataSource(sf, part, partCount, minCustKey, maxCustKey);
        Message message = dataSource.read();
        final Kryo kryo = new Kryo();
        kryo.register(LineOrder.class);
        kryo.register(byte[].class);
        kryo.register(Object[].class);

        final LoaderConfig config = LoaderConfig.INSTANCE();
        try {
            config.init();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        final int capacity = 8;
        final int partitionNum = 8;
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(1000, 1, 1500, 0, 10000000);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        TpchDataTransformer transformer = new TpchDataTransformer();
        ParaflowRecord[][] content = new ParaflowRecord[partitionNum][];
        ParaflowRecord[] records = new ParaflowRecord[capacity];
        //ParaflowRecord[] records = new ParaflowRecord[capacity];
        int counter;
        int i;
        long textSize = 0;
        Output output = new ByteBufferOutput(300, 2000);
        int index = 0;
        while (lineOrderIterator.hasNext()) {
            i = 0;
            while (lineOrderIterator.hasNext() && i < partitionNum) {
                counter = 0;
                while (lineOrderIterator.hasNext() && counter < capacity) {
                    LineOrder lineOrder = lineOrderIterator.next();
                    kryo.writeObject(output, lineOrder);
                    textSize += output.position();
                    ParaflowRecord record = transformer.transform(output.toBytes(), 1);
                    System.out.println(record);
                    records[counter] = record;
                    output.reset();
                    counter++;
                }
                content[i] = sort(records, index);
                i++;
            }
            ParaflowSegment segment = new ParaflowSegment(content, new long[0], new long[0], 0.0d);
            segment.setDb(dbName);
            segment.setTable(tableName);
            String path = config.getMemoryWarehouse() + dbName + "/" + tableName + "/" + "i" + index + "i" + config.getLoaderId() + System.nanoTime() + random.nextInt();
            segment.setPath(path);
            MetaProto.StringListType columnNames = metaClient.listColumns(dbName, tableName);
            MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(dbName, tableName);
            final ParquetSegmentWriter segmentWriter = new ParquetSegmentWriter(segment, metaClient, null);
            long start = System.currentTimeMillis();
            if (segmentWriter.write(segment, columnNames, columnTypes)) {
                System.out.println("Binary size: " + (1.0 * textSize / 1024.0 / 1024.0) + " MB.");
            }
            long end = System.currentTimeMillis();
            System.out.println("Time cost: " + (end - start));
//            flushSegment(segment);
            index ++;
        }
        output.close();
    }

    private ParaflowRecord[] sort(ParaflowRecord[] content, int index) {
        ArrayList<ParaflowRecord> contentArray = new ArrayList<>();
        contentArray.addAll(Arrays.asList(content));
        if (index % 6 == 0) {
            contentArray.sort(Comparator.comparingLong(ParaflowRecord::getTimestamp));
        } else if (index % 6 == 1) {
            contentArray.sort(new Comparator<ParaflowRecord>() {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2) {
                    double totalPrice1 = Double.parseDouble(o1.getValue(3).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(3).toString());
                    if (totalPrice1 > totalPrice2) {
                        return 1;
                    }
                    if (totalPrice1 < totalPrice2) {
                        return -1;
                    }
                    return 0;
                }
            });
        } else if (index % 6 == 2) {
            contentArray.sort(new Comparator<ParaflowRecord>() {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2) {
                    double totalPrice1 = Double.parseDouble(o1.getValue(10).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(10).toString());
                    if (totalPrice1 > totalPrice2) {
                        return 1;
                    }
                    if (totalPrice1 < totalPrice2) {
                        return -1;
                    }
                    return 0;
                }
            });
        } else if (index % 6 == 3) {
            contentArray.sort(new Comparator<ParaflowRecord>() {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2) {
                    double totalPrice1 = Double.parseDouble(o1.getValue(11).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(11).toString());
                    if (totalPrice1 > totalPrice2) {
                        return 1;
                    }
                    if (totalPrice1 < totalPrice2) {
                        return -1;
                    }
                    return 0;
                }
            });
        } else if (index % 6 == 4) {
            contentArray.sort(new Comparator<ParaflowRecord>() {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2) {
                    double totalPrice1 = Double.parseDouble(o1.getValue(12).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(12).toString());
                    if (totalPrice1 > totalPrice2) {
                        return 1;
                    }
                    if (totalPrice1 < totalPrice2) {
                        return -1;
                    }
                    return 0;
                }
            });
        } else if (index % 6 == 5) {
            contentArray.sort(new Comparator<ParaflowRecord>() {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2) {
                    double totalPrice1 = Double.parseDouble(o1.getValue(13).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(13).toString());
                    if (totalPrice1 > totalPrice2) {
                        return 1;
                    }
                    if (totalPrice1 < totalPrice2) {
                        return -1;
                    }
                    return 0;
                }
            });
        }
        int tempSize = contentArray.size();
        ParaflowRecord[] tempRecords = new ParaflowRecord[tempSize];
        contentArray.toArray(tempRecords);
        return tempRecords;
    }

    private void flushSegment(ParaflowSegment segment)
    {
        final LoaderConfig config = LoaderConfig.INSTANCE();
        String segmentPath = segment.getPath();
        int fileNamePoint = segmentPath.lastIndexOf("/");
        int tblPoint = segmentPath.lastIndexOf("/", fileNamePoint - 1);
        int dbPoint = segmentPath.lastIndexOf("/", tblPoint - 1);
        int indexEndPoint = segmentPath.lastIndexOf("i");
        int indexStartPoint = segmentPath.lastIndexOf("i", indexEndPoint - 1);
        int sortColumnId = Integer.parseInt(segmentPath.substring(indexStartPoint + 1, indexEndPoint));
        String suffix = segmentPath.substring(indexEndPoint + 1);
        String newPath = config.getHDFSWarehouse() + suffix;
        Path outputPath = new Path(newPath);
        Configuration configuration = new Configuration(false);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("dfs.replication", "1");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(config.getHDFSWarehouse()), configuration);
            fs.create(outputPath, (short) 1);
            fs.copyFromLocalFile(true, new Path(segmentPath), outputPath);
            // add block index
            long[] fiberMinTimestamps = segment.getFiberMinTimestamps();
            long[] fiberMaxTimestamps = segment.getFiberMaxTimestamps();
            long writeTime = segment.getWriteTime();
            int partitionNum = fiberMinTimestamps.length;
            for (int i = 0; i < partitionNum; i++) {
                if (fiberMinTimestamps[i] == -1) {
                    continue;
                }
                if (fiberMaxTimestamps[i] == -1) {
                    continue;
                }
                metaClient.createBlockIndex(dbName, tableName, i, fiberMinTimestamps[i], fiberMaxTimestamps[i], sortColumnId, newPath);
            }
            double writeLatency = Math.abs(writeTime - segment.getAvgTimestamp());
//            double flushLatency = Math.abs(System.currentTimeMillis() - segment.getAvgTimestamp());
            //logger.info("write latency: " + writeLatency + " ms.");
//            logger.info("flush latency: " + flushLatency + " ms.");
//            if (metricEnabled) {
//                metric.addValue(writeLatency);
//            }
        }
        catch (IOException e) {
            e.printStackTrace();
            if (fs != null) {
                try {
                    fs.deleteOnExit(outputPath);
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}

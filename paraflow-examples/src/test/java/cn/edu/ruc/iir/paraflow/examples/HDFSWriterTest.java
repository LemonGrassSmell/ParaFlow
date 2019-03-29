package cn.edu.ruc.iir.paraflow.examples;

import cn.edu.ruc.iir.paraflow.benchmark.TpchTable;
import cn.edu.ruc.iir.paraflow.benchmark.model.LineOrder;
import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ConfigFileNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.proto.StatusProto;
import cn.edu.ruc.iir.paraflow.loader.ParaflowSegment;
import cn.edu.ruc.iir.paraflow.loader.ParquetSegmentWriter;
import cn.edu.ruc.iir.paraflow.loader.utils.LoaderConfig;
import cn.edu.ruc.iir.paraflow.metaserver.client.MetaClient;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
    private String tableName = "lineorder";
    @Test
    public void testDebug()
    {
        final Random random = new Random(System.nanoTime());
        StatusProto.ResponseStatus expect = StatusProto.ResponseStatus.newBuilder().setStatus(StatusProto.ResponseStatus.State.STATUS_OK).build();
        StatusProto.ResponseStatus status1 = metaClient.createUser("alice1", "123456");
        MetaProto.StringListType dbs = metaClient.listDatabases();
        boolean dbExisted = false;
        for (int i = 0; i < dbs.getStrCount(); i++) {
            if (dbs.getStr(i).equals(dbName)) {
                System.out.println(dbs.getStr(i));
                dbExisted = true;
                break;
            }
        }
        if (!dbExisted) {
            StatusProto.ResponseStatus status = metaClient.createDatabase(dbName, "alice1");
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
            metaClient.createTable(dbName, tableName, "parquet", 1,
                    "custkey%8", 22,
                    Arrays.asList(names), Arrays.asList(types));
        }
        final LoaderConfig config = LoaderConfig.INSTANCE();
        try {
            config.init();
        }
        catch (ConfigFileNotFoundException e) {
            e.printStackTrace();
        }
        final int capacity = 64; //count of records in each block
        final int partitionNum = 8;
        Iterable<LineOrder> lineOrderIterable = TpchTable.LINEORDER.createGenerator(10, 1, 1500, 0, 10000000);
        Iterator<LineOrder> lineOrderIterator = lineOrderIterable.iterator();
        ParaflowRecord[][] content = new ParaflowRecord[partitionNum][partitionNum];
        ArrayList<ParaflowRecord>[] fiberPartitions = new ArrayList[partitionNum];
        ParaflowRecord[] records = new ParaflowRecord[capacity];
        long textSize = 0;
        int fiberValue;
        int fiberIndex;
        long[] fiberMaxTimeStamps = new long[1];
        long[] fiberMinTimeStamps = new long[1];
        int a;
        int b;
        Map<Integer, Integer> fiberCount = new HashMap<>();
        Map<Integer, Integer> sortIndexByFiber = new HashMap<>();
        int m;
        int n;
        int counter;
        Map<Integer, Integer> sortColumnIndex = new HashMap<>();
        sortColumnIndex.put(0, 22);
        sortColumnIndex.put(1, 3);
        sortColumnIndex.put(2, 10);
        sortColumnIndex.put(3, 11);
        sortColumnIndex.put(4, 12);
        sortColumnIndex.put(5, 13);
        long tempTimeStamp;
        ParaflowRecord record;
        for (int j = 0; j < partitionNum; j++) {
            fiberCount.put(j, 0);
        }
        for (int j = 0; j < partitionNum; j++) {
            sortIndexByFiber.put(j, 0);
        }
        while (lineOrderIterator.hasNext()) {
//            textSize = 0;
            record = lineOrderIterator.next();
            long custkey = Long.parseLong(String.valueOf(record.getValue(1)));
            fiberValue = fiberfunction(custkey);
            fiberIndex = fiberCount.get(fiberValue);
            if (fiberIndex == 0) {
                fiberPartitions[fiberValue] = new ArrayList<>();
                fiberPartitions[fiberValue].add(record);
                fiberIndex++;
                fiberCount.put(fiberValue, fiberIndex);
            } else if (fiberIndex < capacity) {
                fiberPartitions[fiberValue].add(record);
                fiberIndex++;
                fiberCount.put(fiberValue, fiberIndex);
            } else {
                //xiang content limian xunhuan fangru fiber zhi xiangtong de record
                try {
                    for (int toArray = 0; toArray < capacity; toArray++) {
                        records[toArray] = fiberPartitions[fiberValue].get(toArray);
                    }
//                    records = (ParaflowRecord[]) fiberPartitions[fiberValue].toArray();
                    records = sort(records, sortIndexByFiber.get(fiberValue));
                } catch (Exception e) {
                    System.out.println("wrong!!!");
                }
                counter = 0;
                fiberMaxTimeStamps[0] = 0;
                fiberMinTimeStamps[0] = Long.parseLong(String.valueOf(records[0].getValue(22)));
                for (m = 0; m < partitionNum; m++) {
                    for (n = 0; n < partitionNum; n++) {
                        content[m][n] = records[counter];
                        tempTimeStamp = Long.parseLong(String.valueOf(records[counter].getValue(22)));
                        if (tempTimeStamp > fiberMaxTimeStamps[0]) {
                            fiberMaxTimeStamps[0] = tempTimeStamp;
                        }
                        if (tempTimeStamp < fiberMinTimeStamps[0]) {
                            fiberMinTimeStamps[0] = tempTimeStamp;
                        }
                        counter++;
                    }
                }
                ParaflowSegment segment = new ParaflowSegment(content, new long[0], new long[0], 0.0d);
                segment.setDb(dbName);
                segment.setTable(tableName);
                String path = config.getMemoryWarehouse() + dbName + "/" + tableName + "/" + config.getLoaderId() + System.nanoTime() + random.nextInt();
                segment.setPath(path);
                segment.setSortColId(sortColumnIndex.get(sortIndexByFiber.get(fiberValue) % 6));
                Map<Integer, Integer> fiberCountFull = new HashMap<>();
                for (int j = 0; j < partitionNum; j++) {
                    fiberCountFull.put(j, partitionNum);
                }
                segment.setfiberCount(fiberCountFull);
                segment.setFiberMaxTimestamps(fiberMaxTimeStamps);
                segment.setFiberMinTimestamps(fiberMinTimeStamps);
                segment.setPartitionNum(partitionNum);
                segment.setFiberValue(fiberValue);
                MetaProto.StringListType columnNames = metaClient.listColumns(dbName, tableName);
                MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(dbName, tableName);
                final ParquetSegmentWriter segmentWriter = new ParquetSegmentWriter(segment, metaClient, null);
                long start = System.currentTimeMillis();
                if (segmentWriter.write(segment, columnNames, columnTypes)) {
                    System.out.println("Binary size: " + (1.0 * textSize / 1024.0 / 1024.0) + " MB.");
                }
                long end = System.currentTimeMillis();
                System.out.println("Time cost: " + (end - start));
                flushSegment(segment);
                fiberPartitions[fiberValue].clear();
                fiberCount.put(fiberValue, 0);
                sortIndexByFiber.put(fiberValue, sortIndexByFiber.get(fiberValue) + 1);
            }
        }
        System.out.println("==========================================");
        for (int partition = 0; partition < partitionNum; partition++) {
            Map<Integer, Integer> fiberCountNotFull = new HashMap<>();
            for (int j = 0; j < partitionNum; j++) {
                fiberCountNotFull.put(j, 0);
            }
            try {
                records = null;
                records = new ParaflowRecord[fiberPartitions[partition].size()];
                for (int toArray = 0; toArray < records.length; toArray++) {
                    records[toArray] = fiberPartitions[partition].get(toArray);
                }
//                records = (ParaflowRecord[]) fiberPartitions[partition].toArray();
                records = sort(records, sortIndexByFiber.get(partition));
            } catch (Exception e) {
                System.out.println("wrong!!!");
            }
            fiberMaxTimeStamps[0] = 0;
            fiberMinTimeStamps[0] = Long.parseLong(String.valueOf(records[0].getValue(22)));
            counter = 0;
            a = 0;
            b = 0;
            content = null;
            content = new ParaflowRecord[partitionNum][partitionNum];
            while (counter < records.length) {
                tempTimeStamp = Long.parseLong(String.valueOf(records[counter].getValue(22)));
                if (tempTimeStamp > fiberMaxTimeStamps[0]) {
                    fiberMaxTimeStamps[0] = tempTimeStamp;
                }
                if (tempTimeStamp < fiberMinTimeStamps[0]) {
                    fiberMinTimeStamps[0] = tempTimeStamp;
                }
                if (a < partitionNum) {
                    if (b < partitionNum - 1) {
                        content[a][b++] = records[counter++];
                        fiberCountNotFull.put(a, fiberCountNotFull.get(a)+1);
                    }
                    else {
                        content[a][b] = records[counter++];
                        fiberCountNotFull.put(a, fiberCountNotFull.get(a)+1);
                        b = 0;
                        a++;
                    }
                }
                else {
                    break;
                }
            }
            ParaflowSegment segment = new ParaflowSegment(content, new long[0], new long[0], 0.0d);
            segment.setDb(dbName);
            segment.setTable(tableName);
            String path = config.getMemoryWarehouse() + dbName + "/" + tableName + "/" + config.getLoaderId() + System.nanoTime() + random.nextInt();
            segment.setPath(path);
            segment.setSortColId(sortColumnIndex.get(sortIndexByFiber.get(partition) % 6));
            segment.setfiberCount(fiberCountNotFull);
            segment.setPartitionNum(partitionNum);
            segment.setFiberMaxTimestamps(fiberMaxTimeStamps);
            segment.setFiberMinTimestamps(fiberMinTimeStamps);
            segment.setFiberValue(partition);
            MetaProto.StringListType columnNames = metaClient.listColumns(dbName, tableName);
            MetaProto.StringListType columnTypes = metaClient.listColumnsDataType(dbName, tableName);
            final ParquetSegmentWriter segmentWriter = new ParquetSegmentWriter(segment, metaClient, null);
            long start = System.currentTimeMillis();
            if (segmentWriter.write(segment, columnNames, columnTypes)) {
                System.out.println("Binary size: " + (1.0 * textSize / 1024.0 / 1024.0) + " MB.");
            }
            long end = System.currentTimeMillis();
            System.out.println("Time cost: " + (end - start));
            flushSegment(segment);
        }
    }

    private int fiberfunction(long customerKey)
    {
        return (int) (customerKey % 8);
    }

    private void flushSegment(ParaflowSegment segment)
    {
        final LoaderConfig config = LoaderConfig.INSTANCE();
        String segmentPath = segment.getPath();
        int fileNamePoint = segmentPath.lastIndexOf("/");
        int tblPoint = segmentPath.lastIndexOf("/", fileNamePoint - 1);
        int dbPoint = segmentPath.lastIndexOf("/", tblPoint - 1);
        int sortColumnId = segment.getSortColId();
        String suffix = segmentPath.substring(dbPoint + 1);
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
            int partitionNum = fiberMinTimestamps.length;
            int fiberValue = segment.getFiberValue();
//            for (int i = 0; i < partitionNum; i++) {
//                if (fiberMinTimestamps[i] == -1) {
//                    continue;
//                }
//                if (fiberMaxTimestamps[i] == -1) {
//                    continue;
//                }
//                metaClient.createBlockIndex(dbName, tableName, i, fiberMinTimestamps[i], fiberMaxTimestamps[i], sortColumnId, newPath);
//            }
            metaClient.createBlockIndex(dbName, tableName, fiberValue, fiberMinTimestamps[0], fiberMaxTimestamps[0], sortColumnId, newPath);
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

    //from small to big sort
    private ParaflowRecord[] sort(ParaflowRecord[] content, int index)
    {
        ArrayList<ParaflowRecord> contentArray = new ArrayList<>();
        contentArray.addAll(Arrays.asList(content));
        if (index % 6 == 0) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
                    double totalPrice1 = Double.parseDouble(o1.getValue(22).toString());
                    double totalPrice2 = Double.parseDouble(o2.getValue(22).toString());
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
        else if (index % 6 == 1) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
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
        }
        else if (index % 6 == 2) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
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
        }
        else if (index % 6 == 3) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
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
        }
        else if (index % 6 == 4) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
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
        }
        else if (index % 6 == 5) {
            contentArray.sort(new Comparator<ParaflowRecord>()
            {
                @Override
                public int compare(ParaflowRecord o1, ParaflowRecord o2)
                {
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
}

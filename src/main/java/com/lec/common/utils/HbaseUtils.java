package com.lec.common.utils;

import com.lec.driver.LabelDriver;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class HbaseUtils {

    private Connection conn = null;

    public HbaseUtils(Connection conn) {
        this.conn = conn;
    }

    private final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public Boolean isTableExist(String tableName) throws IOException {
        //获取Admin对象
        Admin admin = conn.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return result;
    }

    /**
     * 获取2位随机数
     *
     * @return
     */
    public String getRandomNumber() {
        String ranStr = Math.random() + "";
        int pointIndex = ranStr.indexOf(".");
        return ranStr.substring(pointIndex + 1, pointIndex + 3);
    }

    /**
     * 使用hash值获取两位数
     *
     * @param value
     * @param number
     * @return
     */
    public String evaluate(String value, long number) {
        if (value == null || value.toString().equals("") || number < 1) {
            return null;

        } else {
            long result = Math.abs(value.hashCode() % number);

            int formatLength = String.valueOf(number - 1).length();

            String newString = String.valueOf(result).substring(0, 2);

            return newString;

        }

    }

    /**
     * 插入一万条数据
     *
     * @return
     */
    private List<Put> batchPut() {
        ArrayList<Put> list = new ArrayList<Put>();
        for (int i = 0; i < 10000; i++) {
            byte[] rowkey = Bytes.toBytes(getRandomNumber() + "_" + System.currentTimeMillis() + "_" + i);
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zs" + i));
            list.add(put);
        }
        return list;
    }

    /**
     * 预分区的范围
     *
     * @return
     */
    public byte[][] getSplitKeys(int size) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < size; i++) {
            if (i < 10)
                stringBuffer.append("0" + i + "|,");
            else if (i < size - 1)
                stringBuffer.append(i + "|,");
            else
                stringBuffer.append(i + "|");

        }
        //System.out.println(stringBuilder.toString());
        String[] keys = stringBuffer.toString().split(",");

        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowkeyIter = rows.iterator();
        int i = 0;
        while (rowkeyIter.hasNext()) {
            byte[] tempRow = rowkeyIter.next();
            rowkeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;

    }

    /**
     * 创建预分区的表
     *
     * @param tableName
     * @param columFamily
     * @throws IOException
     */
    public void createTableBySplitKeys(String tableName, List<String> columFamily, int size) throws IOException {
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已经存在");
        } else {
            Admin admin = conn.getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            byte[][] splitKeys = getSplitKeys(size);
            admin.createTable(hTableDescriptor, splitKeys);
            admin.close();
        }

    }

    /***
     * 创建预分区禁止分裂
     * @param tableName
     * @param columFamily
     * @param size
     * @throws IOException
     */
    public void createTableBySplitPolicy(String tableName, List<String> columFamily, int size) throws IOException {
        if (isTableExist(tableName)) {
            LOG.info("------ 表已经存在 执行删除-------");
            deleteTable(tableName);
            LOG.info("------ 删除完毕----------------");
        }
        Admin admin = conn.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        hTableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());// 指定策略

        for (String cf : columFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        byte[][] splitKeys = getSplitKeys(size);
        admin.createTable(hTableDescriptor, splitKeys);
        admin.close();


    }

    /**
     * 设置表的列簇存活时长
     *
     * @param tableName
     * @param columFamily
     * @param size
     * @throws Exception
     */
    public void createTableByTimeToLive(String tableName, List<String> columFamily, int size) throws IOException {
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已经存在");
        } else {
            Admin admin = conn.getAdmin();
            HColumnDescriptor hColumnDescriptor = null;
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                hTableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());// 指定策略

                for (String cf : columFamily) {
                    hColumnDescriptor = new HColumnDescriptor(cf);
                    hColumnDescriptor.setTimeToLive(32832000);
                    hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
                    hTableDescriptor.addFamily(hColumnDescriptor);

                }
//            HColumnDescriptor hcd = new HColumnDescriptor("m");
//            hcd.setTimeToLive(345600); // 设置TTL过期时间4天 4 * 24 * 60 * 60

//            hTableDescriptor.addFamily(hColumnDescriptor);
                byte[][] splitKeys = getSplitKeys(size);
                admin.createTable(hTableDescriptor, splitKeys);
            }
        }
    }


    public void mofityTableBySplitPolicy(String tableName, List<String> columFamily) throws IOException {

        Admin admin = conn.getAdmin();
        TableName tableName1 = TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);

        hTableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class.getName());// 指定策略
        hTableDescriptor.setValue("KeyPrefixRegionSplitPolicy.prefix_length", "2");

        for (String cf : columFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.disableTable(tableName1);
        admin.modifyTable(tableName1, hTableDescriptor);
        admin.enableTable(tableName1);
        admin.close();
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void createTable(String tableName, String... columnFamilies) throws IOException {
        //0判断表存不存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
        } else {
            //1.获取admin
            Admin admin = conn.getAdmin();
            //2.创建表
            //2.1创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //2.2创建列描述器
            for (String columnFamily : columnFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            admin.createTable(hTableDescriptor);
            admin.close();
        }
    }

    /**
     * 修改列的版本
     *
     * @param tableName
     * @param columnFamily
     * @param versions
     * @throws IOException
     */
    public void modifyColumnVersions(String tableName, String columnFamily, int versions) throws IOException {
        if (isTableExist(tableName)) {
            Admin admin = conn.getAdmin();
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hColumnDescriptor.setMaxVersions(versions);
            admin.modifyColumn(TableName.valueOf(tableName), hColumnDescriptor);
            admin.close();
        } else {
            System.out.println(tableName + "表不存在");
        }
    }


    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public void deleteTable(String tableName) throws IOException {

        if (isTableExist(tableName)) {
            Admin admin = conn.getAdmin();

            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            admin.close();

        } else {
            System.out.println(tableName + "表不存在");
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public void putDataColumn(String tableName, String rowkey, String columnFamily, String column, String value) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } else {
            System.out.println(tableName + "表不存在");
        }

    }

    public void putData(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        table.put(batchPut());
        table.close();

    }


    /**
     * 获取一行数据
     *
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public void getRow(String tableName, String rowkey) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setMaxVersions();
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] familyByte = CellUtil.cloneFamily(cell);
                byte[] qualifierByte = CellUtil.cloneQualifier(cell);
                byte[] valueByte = CellUtil.cloneValue(cell);
                long timestamp = cell.getTimestamp();

                System.out.println(
                        rowkey + "," +
                                Bytes.toString(familyByte) + ":" +
                                Bytes.toString(qualifierByte) + "," +
                                timestamp + "," +
                                Bytes.toString(valueByte));

            }
            table.close();

        } else {
            System.out.println(tableName + "表不存在");
        }
    }

    /**
     * 扫描全表数据
     *
     * @param tableName
     * @throws IOException
     */
    public void getScan(String tableName) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    byte[] familyByte = CellUtil.cloneFamily(cell);
                    byte[] qualifierByte = CellUtil.cloneQualifier(cell);
                    byte[] valueByte = CellUtil.cloneValue(cell);
                    byte[] rowkeyByte = CellUtil.cloneRow(cell);
                    System.out.println(
                            Bytes.toString(rowkeyByte) + "," +
                                    Bytes.toString(familyByte) + ":" +
                                    Bytes.toString(qualifierByte) + "," +
                                    Bytes.toString(valueByte));

                }
            }
            table.close();
        } else {
            System.out.println(tableName + "表不存在");
        }
    }


    /**
     * 通过过滤器筛选数据
     *
     * @param tableName
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public void getDataByFilter(String tableName, String columnFamily, String column, String value) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();

            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(column),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(value));
            singleColumnValueFilter.setFilterIfMissing(true);
            scan.setFilter(singleColumnValueFilter);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    byte[] familyByte = CellUtil.cloneFamily(cell);
                    byte[] qualifierByte = CellUtil.cloneQualifier(cell);
                    byte[] valueByte = CellUtil.cloneValue(cell);
                    byte[] rowkeyByte = CellUtil.cloneRow(cell);
                    System.out.println(
                            Bytes.toString(rowkeyByte) + "," +
                                    Bytes.toString(familyByte) + ":" +
                                    Bytes.toString(qualifierByte) + "," +
                                    Bytes.toString(valueByte));

                }
            }
            table.close();
        } else {
            System.out.println(tableName + "表不存在");
        }
    }

    public void deleteRow(String tableName, String rowkey) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            table.close();
        } else {
            System.out.println(tableName + "表已存在");
        }
    }

    public void deleteByColumn(String tableName, String rowkey, String columnFamily, String column) throws IOException {
        if (isTableExist(tableName)) {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            table.delete(delete);
            table.close();
        } else {
            System.out.println(tableName + "表已存在");
        }

    }

    public void createHBaseTableByPartition(String tableName, String columnName, int size, String flag) throws IOException {
        ArrayList<String> list = new ArrayList<String>();
        if (flag == "true" || flag.equals("true")) {

            if (columnName.split("#").length == 1) {
                list.add(columnName);
            } else {
                String[] strings = columnName.split("#");
                for (String string : strings) {
                    list.add(string);
                }
            }
            this.createTableBySplitPolicy(tableName, list, size);
        } else if (flag == "live" || flag.equals("live")) {
            if (columnName.split("#").length == 1) {
                list.add(columnName);
            } else {
                String[] strings = columnName.split("#");
                for (String string : strings) {
                    list.add(string);
                }
            }
            this.createTableByTimeToLive(tableName, list, size);
        } else {
            list.add(columnName);
            this.createTableBySplitKeys(tableName, list, size);
        }

    }

    public void main(String[] args) throws IOException {

        Admin admin = conn.getAdmin();
        HColumnDescriptor info = new HColumnDescriptor("info");
        HTableDescriptor staff = new HTableDescriptor(TableName.valueOf("staff"));
        staff.addFamily(info);
        byte[][] spiltKeys = new byte[3][];
        spiltKeys[0] = Bytes.toBytes("1000");
        spiltKeys[1] = Bytes.toBytes("2000");
        spiltKeys[2] = Bytes.toBytes("3000");
        admin.createTable(staff, spiltKeys);
        admin.close();


    }
}

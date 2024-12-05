package com.example.dw.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @Description: HBase工具类
 * @Author: Chenyang on 2024/11/28 16:00
 * @Version: 1.0
 */
public class HBaseUtils {

    private static Connection hbaseConn = null;

    // 获取连接
    public static Connection getHBaseConnection() throws IOException {
        if (hbaseConn == null || hbaseConn.isClosed()) {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop212,hadoop213,hadoop214");
            hbaseConn = ConnectionFactory.createConnection(conf);
        }
        return hbaseConn;
    }

    // 关闭连接
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    /**
     * 获取异步操作HBase的连接对象
     *
     * @return
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop212,hadoop213,hadoop214");
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭异步操作HBase的连接对象
     *
     * @param asyncConn
     */
    public static void closeHBaseAsyncConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 创建表
    public static void createTable(Connection hbaseConn, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("====== 至少需要一个列族 ======");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("====== 命名空间" + namespace + "下的表" + tableName + "已存在 ======");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);
            System.out.println("====== 命名空间" + namespace + "下的表" + tableName + "已创建 ======");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    // 删除表
    public static void deleteTable(Connection hbaseConn, String namespace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("====== 命名空间" + namespace + "下的表" + tableName + " 不存在 ======");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("====== 命名空间" + namespace + "下的表" + tableName + " 已删除 ======");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObject) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObject.keySet();
            for (String column : columns) {
                String value = jsonObject.getString(column);
                if (StringUtils.isNotBlank(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("====== 向命名空间" + namespace + "下的表" + tableName + " 新增数据成功 ======");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 删除数据
    public static void deleteRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("====== 向命名空间" + namespace + "下的表" + tableName + " 删除数据成功 ======");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 读取表中的一行数据
    public static <T> T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey, Class<T> clazz, boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                T obj = clazz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 异步获取一行数据
     *
     * @param asyncConn
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param clazz
     * @param isUnderlineToCamel
     * @param <T>
     * @return
     */
    public static <T> T getRowAsync(AsyncConnection asyncConn, String namespace, String tableName, String rowKey, Class<T> clazz, boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try {
            AsyncTable<AdvancedScanResultConsumer> table = asyncConn.getTable(tableNameObj);

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                T obj = clazz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Connection hbaseConn = getHBaseConnection();
        JSONObject row = getRow(hbaseConn, "dw_realtime", "dim_base_trademark", "1", JSONObject.class, true);
        System.out.println(row);
        closeHBaseConnection(hbaseConn);
    }

}

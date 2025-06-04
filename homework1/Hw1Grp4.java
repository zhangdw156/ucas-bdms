import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Hw1Grp4 {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: Hw1Grp4 <input>");
            System.exit(1);
        }

        String file = args[0].split("=")[1];
        int rowKey = Integer.parseInt(args[1].split(":")[1].split(",")[0].replace("R", ""));
        String operator = args[1].split(":")[1].split(",")[1];
        double value = Double.parseDouble(args[1].split(":")[1].split(",")[2]);
        String[] columnName = args[2].split(":")[1].split(",");
        int[] columnIndex = Arrays.stream(columnName)
                .map(s -> s.replace("R", ""))
                .mapToInt(Integer::parseInt)
                .toArray();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

        String tableName = "Result";
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);

        if (hAdmin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            hAdmin.createTable(htd);
            System.out.println("table " + tableName + " created successfully");
        }
        hAdmin.close();

        HTable table = new HTable(configuration, tableName);
        Set<String> hashSet = new HashSet<>();
        int count = 1;
        String s;

        while ((s = in.readLine()) != null) {
            List<String> tmp = Arrays.asList(s.split("\\|"));
            if (shouldProcessRow(tmp.get(rowKey), operator, value)) {
                String rowKeyString = buildRowKey(tmp, columnIndex);
                if (!hashSet.contains(rowKeyString)) {
                    hashSet.add(rowKeyString);
                    Put put = createPut(count, tmp, columnName, columnIndex);
                    table.put(put);
                    count++;
                }
            }
        }

        table.close();
        in.close();
        fs.close();
    }

    private static boolean shouldProcessRow(String rowValue, String operator, double value) {
        double numericValue = Double.parseDouble(rowValue);
        switch (operator) {
            case "gt": return numericValue > value;
            case "lt": return numericValue < value;
            case "eq": return numericValue == value;
            case "ge": return numericValue >= value;
            case "le": return numericValue <= value;
            case "ne": return numericValue != value;
            default:
                System.err.println("Invalid operator!");
                System.exit(1);
                return false;
        }
    }

    private static String buildRowKey(List<String> rowData, int[] columnIndex) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnIndex.length; i++) {
            if (i > 0) sb.append("~");
            sb.append(rowData.get(columnIndex[i]));
        }
        return sb.toString();
    }

    private static Put createPut(int count, List<String> rowData, String[] columnName, int[] columnIndex) {
        Put put = new Put(String.valueOf(count).getBytes());
        for (int i = 0; i < columnIndex.length; i++) {
            put.add("res".getBytes(), columnName[i].getBytes(), rowData.get(columnIndex[i]).getBytes());
        }
        return put;
    }
}
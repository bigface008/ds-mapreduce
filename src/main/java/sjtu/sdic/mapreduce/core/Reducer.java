package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Key;
import java.util.*;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * doReduce manages one reduce task: it should read the intermediate
     * files for the task, sort the intermediate key/value pairs by key,
     * call the user-defined reduce function {@code reduceF} for each key,
     * and write reduceF's output to disk.
     * <p>
     * You'll need to read one intermediate file from each map task;
     * {@code reduceName(jobName, m, reduceTask)} yields the file
     * name from map task m.
     * <p>
     * Your {@code doMap()} encoded the key/value pairs in the intermediate
     * files, so you will need to decode them. If you used JSON, you can refer
     * to related docs to know how to decode.
     * <p>
     * In the original paper, sorting is optional but helpful. Here you are
     * also required to do sorting. Lib is allowed.
     * <p>
     * {@code reduceF()} is the application's reduce function. You should
     * call it once per distinct key, with a slice of all the values
     * for that key. {@code reduceF()} returns the reduced value for that
     * key.
     * <p>
     * You should write the reduce output as JSON encoded KeyValue
     * objects to the file named outFile. We require you to use JSON
     * because that is what the merger than combines the output
     * from all the reduce tasks expects. There is nothing special about
     * JSON -- it is just the marshalling format we chose to use.
     * <p>
     * Your code here (Part I).
     *
     * @param jobName    the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile    write the output here
     * @param nMap       the number of map tasks that were run ("M" in the paper)
     * @param reduceF    user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceF) {
        try {
            System.out.println("Reducer.doReduce(): jobName " + jobName + "; reduceTask "
                    + reduceTask + "; outFile " + outFile + "; nMap " + nMap);
            ArrayList<KeyValue> kvl_slice = new ArrayList<KeyValue>();
            for (int i = 0; i < nMap; i++) {
                System.out.println("Reducer.doReduce(): reading file " + i);
                File file = new File(Utils.reduceName(jobName, i, reduceTask));
                if (!file.exists()) {
                    System.out.println("Reducer.doReduce(): Operation of opening file failed.");
                    return;
                }

                String content = FileUtils.readFileToString(file);
                List<KeyValue> kvl_per_file = JSON.parseArray(content, KeyValue.class);
                kvl_slice.addAll(kvl_per_file);
                System.out.println("Reducer.doReduce(): reading file " + i + " complete");
            }

            System.out.println("Reducer.doReduce(): Sort kvl.");
            kvl_slice.sort(new Comparator<KeyValue>() {
                @Override
                public int compare(KeyValue o1, KeyValue o2) {
                    return o1.key.compareTo(o2.key);
                }
            });
            System.out.println("Reducer.doReduce(): kvl_slice len " + kvl_slice.size());
            System.out.println("Reducer.doReduce(): Sort kvl end.\nReducer.doReduce(): Build result_kvl.");

            int len = kvl_slice.size();
            ArrayList<KeyValue> result_kvl = new ArrayList<KeyValue>();
            ArrayList<String> vl_for_key = new ArrayList<String>();
            for (int i = 0; i < len; i++) {
                if (i != 0 && !(kvl_slice.get(i).key.equals(kvl_slice.get(i - 1)))) {
                    String temp = reduceF.reduce(kvl_slice.get(i - 1).key, vl_for_key.toArray(new String[0]));
                    result_kvl.add(new KeyValue(kvl_slice.get(i - 1).key, temp));
                    vl_for_key.clear();
                }
                vl_for_key.add(kvl_slice.get(i).value);
            }
            if (!vl_for_key.isEmpty()) { /* If values of the last key remain to be pushed. */
                String temp = reduceF.reduce(kvl_slice.get(len - 1).key, vl_for_key.toArray(new String[0]));
                result_kvl.add(new KeyValue((kvl_slice.get(len - 1).key), temp));
            }
            System.out.println("Reducer.doReduce(): build result_kvl end.");

            File file = new File(outFile);
            if (!file.createNewFile()) {
                System.out.println("Reducer.doReduce(): Operation of creating file failed.");
                return;
            }

            FileWriter file_writer = new FileWriter(file.getName());
            file_writer.write(JSON.toJSONString(result_kvl));
            file_writer.close();

            System.out.println("Reducer.doReduce(): reduceTask " + reduceTask + " end.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

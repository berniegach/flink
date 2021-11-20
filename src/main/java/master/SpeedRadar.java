package master;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

public class SpeedRadar implements FlatMapFunction<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {
    /*
    *     for the speed radar we only need Time, VID, XWay, Seg, Dir, Spd
     */
    @Override
    public void flatMap(String s, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {
        String[] array = s.split(",");
        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out =
                new Tuple6<>(Integer.parseInt(array[0]),
                        Integer.parseInt(array[1]),
                        Integer.parseInt(array[3]),
                        Integer.parseInt(array[6]),
                        Integer.parseInt(array[5]),
                        Integer.parseInt(array[2]));
        if(Integer.parseInt(array[2]) > 90 )
            collector.collect(out);
    };
}

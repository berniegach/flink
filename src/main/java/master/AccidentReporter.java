package master;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter {
    public static class SourceFilter implements FlatMapFunction<String, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
    {
        /*
        * Filter out those data we dont need
         */
        @Override
        public void flatMap(String s, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception
        {
            String[] array = s.split(",");
            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> out =
                    new Tuple7<>(Integer.parseInt(array[0]), //Time 0
                            Integer.parseInt(array[1]), //VID 1
                            Integer.parseInt(array[3]), //XWay 2
                            Integer.parseInt(array[6]), //seg 3
                            Integer.parseInt(array[5]), //Dir 4
                            Integer.parseInt(array[7]), //Pos 5
                            Integer.parseInt(array[2])); //Spd 6
            //if(out.f6 == 0)
                collector.collect(out);
        }
    }
    public static class TimeStampAscending extends AscendingTimestampExtractor<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
    {

        @Override
        public long extractAscendingTimestamp(Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) {
            return in.f0*1000;
        }
    }
    public static class AccidentCheck implements WindowFunction<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Integer, TimeWindow>
    {
        public void apply(Integer key, TimeWindow timeWindow, Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception
        {
            int count = 0;
            Integer firstTime = 0;
            int lastTime = 0;
            int VID = 0, XWay = 0, Seg = 0, Dir = 0, Pos = 0;
            boolean posSame = true;
            for(Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> in: input)
            {
                if(count == 0)
                {
                    firstTime = in.f0;
                    VID = in.f1;
                    XWay = in.f2;
                    Seg = in.f3;
                    Dir = in.f4;
                    Pos = in.f5;
                }
                else if(lastTime < in.f0)
                {
                    lastTime = in.f0;
                    if(Pos != in.f5)
                        posSame = false;
                }
                count +=1;
            }
            if(count == 4 && posSame)
            {
                //Time1, Time2, VID, XWay, Seg, Dir, Pos,
                out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer,Integer, Integer>(firstTime, lastTime,VID,XWay,Seg,Dir,Pos));

            }
        }
    }

}

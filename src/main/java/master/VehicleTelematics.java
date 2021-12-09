package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class VehicleTelematics
{
    public static void main(String[] args) throws Exception
    {
        //lets get the command line parameters passed to the program
        //since the files are not given using the format --input file, we use the arguments
        String inputFile = args[0];
        String outputPath = args[1];
        //Since the output path can end with either / or no, we remove it for consistency when making the output file names
        if(outputPath.endsWith("/"))
        {
            outputPath = outputPath.substring(0,outputPath.length() - 1);
        }

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        * Input file format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
        * 1. Time - a timestamp (integer) in seconds identifying the time at which the position event was emitted,
        * 2. VID - integer that identifies the vehicle,
        * 3. Spd - (0 - 100) is an integer that represents the speed in mph (miles per hour),
        * 4. XWay - (0 . . .L−1) identifies the highway from which the position report is emitted
        * 5. Lane - (0 . . . 4) identifies the lane of the highway from which the position report is emitted (0 if it is an
            entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT)).
        * 6. Dir - (0 . . . 1) indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
        * 7. Seg - (0 . . . 99) identifies the segment from which the position report is emitted
        * 8. Pos - (0 . . . 527999) identifies the horizontal position of the vehicle as the number of meters from the
            westernmost point on the highway (i.e., Pos = x)
         */
        //********************************************************************************************************************************************
        //1.SpeedRadar: detects cars that overcome the speed limit of 90 mph.
        //Specify the data source as the csv file containing vehicle information
        DataStream<String> text = env.readTextFile(inputFile);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadarMapStream = text.flatMap(new SpeedRadar());
        speedRadarMapStream.writeAsCsv(outputPath + "/speedfines.csv").setParallelism(1);
        //**********************************************************************************************************************************************

        //3.AccidentReporter. detects stopped vehicles on any segment. A vehicle is stopped when it reports
        //at least 4 consecutive events from the same position
        //Specify the data source as the csv file containing vehicle information
        env.setParallelism(1);
        DataStream<String> text_3 = env.readTextFile(inputFile);
        //filter since we are only interested in Time, VID, XWay, Seg, Dir, Pos
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentReporterMapStream = text_3.flatMap(new AccidentReporter.SourceFilter());
        //Create a sliding window that is event driven
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window
                = accidentReporterMapStream
                .assignTimestampsAndWatermarks(new AccidentReporter.TimeStampAscending());
        env.setParallelism(3);
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> window2 = window
                .keyBy(value -> value.f1)
                .window(SlidingEventTimeWindows.of(Time.seconds(120),Time.seconds(30)))
                .apply(new AccidentReporter.AccidentCheck());

        //export the data to excel
        window2.writeAsCsv(outputPath+"/accidents.csv").setParallelism(1);
        //**********************************************************************************************************************************************
        // execute program
        env.execute("SourceSink");
        //*************************************************************************************************************************

    }

}

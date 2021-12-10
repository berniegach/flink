package master;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
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
        //2.AverageSpeedControl: detects cars with an average speed higher than 60 mph between segments 52 and 56.
        DataStream<String> carData = env.readTextFile(inputFile);
        DataStream<CarEvent> events = carData
                .map(new MapFunction<String, CarEvent>() {
                    @Override
                    public CarEvent map(String line) throws Exception {
                        return CarEvent.fromString(line);
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<CarEvent>() {
                            @Override
                            public long extractAscendingTimestamp(CarEvent element) {
                                return element.timestamp*1000;
                            }
                        }
                )
                .filter(r -> r.segment >= 52 && r.segment <= 56);

        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> avgEvents = events
                .keyBy((CarEvent r) -> r.vehicleID)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .allowedLateness(Time.seconds(60))
                .process(new AvgSpeedControl());

        avgEvents.writeAsCsv(outputPath + "/avgspeedfines.csv").setParallelism(1);
        //********************************************************************************************************************************************

        // execute program
        env.execute("SourceSink");
        //*************************************************************************************************************************

    }
    public static class AvgSpeedControl extends ProcessWindowFunction<CarEvent, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Integer, TimeWindow> {

        @Override
        public void process(Integer vehicleID, Context context, Iterable<CarEvent> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

            boolean firstSeg = false, secondSeg = false, thirdSeg = false, fourthSeg = false, lastSeg = false;
            int firstTime = 0, lastTime = 0, firstPos = 0, lastPos = 0, dir = 0, hw = 0;

            for (CarEvent e : input) {

                if (e.direction == 0) {
                    if (firstPos == 0 && e.segment == 52) {
                        firstSeg = true;
                        firstTime = e.timestamp;
                        firstPos = e.position;
                    }
                    if (e.segment == 53) secondSeg = true;
                    if (e.segment == 54) thirdSeg = true;
                    if (e.segment == 55) fourthSeg = true;
                    if (e.segment == 56) {
                        lastSeg = true;
                        lastTime = e.timestamp;
                        lastPos = e.position;
                        dir = e.direction;
                        hw = e.highway;
                    }
                }
                else {
                    if (lastPos == 0 && e.segment == 56) {
                        firstSeg = true;
                        firstTime = e.timestamp;
                        lastPos = e.position; // So that the speed formula work
                    }
                    if (e.segment == 55) secondSeg = true;
                    if (e.segment == 54) thirdSeg = true;
                    if (e.segment == 53) fourthSeg = true;
                    if (e.segment == 52) {
                        lastSeg = true;
                        lastTime = e.timestamp;
                        firstPos = e.position;
                        dir = e.direction;
                        hw = e.highway;
                    }
                }
            }

            if (firstSeg && secondSeg && thirdSeg && fourthSeg && lastSeg) {

                // Speed = Distance traveled / Time taken
                double calc = (lastPos - firstPos) / (lastTime - firstTime);
                // Miles per hour = meters per second * 2.236936
                int avgSpeed = (int)(calc * 2.236936);

                if (avgSpeed > 60) out.collect(Tuple6.of(firstTime, lastTime, vehicleID, hw, dir, avgSpeed));
            }
        };
    }


}

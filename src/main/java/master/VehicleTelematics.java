package master;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        // execute program
        env.execute("SourceSink");

    }
}

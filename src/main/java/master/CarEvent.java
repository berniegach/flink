package master;
public class CarEvent {
    
    public int timestamp;
    public int vehicleID;
    public int speed;
    public int highway;
    public int lane;
    public int direction;
    public int segment;
    public int position;

    public CarEvent() {}

    public static CarEvent fromString(String line) {
        
        String[] tokens = line.split(",");
        if (tokens.length != 8){
            throw new RuntimeException("Invalid record " + line);
        }

        CarEvent event = new CarEvent();

        try {
            event.timestamp = Integer.parseInt(tokens[0]);
            event.vehicleID = Integer.parseInt(tokens[1]);
            event.speed = Integer.parseInt(tokens[2]);
            event.highway = Integer.parseInt(tokens[3]);
            event.lane = Integer.parseInt(tokens[4]);
            event.direction = Integer.parseInt(tokens[5]);
            event.segment = Integer.parseInt(tokens[6]);
            event.position = Integer.parseInt(tokens[7]);
        } catch (Exception e) {
            throw new RuntimeException("Invalid field: " + line);
        }

        return event;
    }
}

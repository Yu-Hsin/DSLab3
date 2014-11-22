import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class MapperJob implements Runnable {

    private String mapperClass = "TestMapper", mapperFunction = "map"; 
    private int numReducer;
    MapReduceTask mTask;
    
    private String[] reducerIP;
    private static int port2reducer = 2000;

    private String localFnName = "";
    private Socket toMasterSocket;

    Socket socket;

    public MapperJob(Socket s) {
	socket = s;
    }

    @Override
    public void run() {
	// TODO Auto-generated method stub
	getInitialInfo();
    }

    public void getInitialInfo() {
	try {
	    
	    System.out.println("Getting initial information for current task...");
	    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

	    Object obj = ois.readObject();
	    if (obj instanceof MapReduceTask) {
		mTask = (MapReduceTask) obj;
		numReducer = mTask.getReducerNum();
		mapperClass = mTask.getMapperClass();
		mapperFunction = mTask.getMapperFunc();
	    }
	    System.out.println(numReducer);
	    System.out.println(mapperClass);
	    System.out.println(mapperFunction);
	    
	    ois.close();
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }
}

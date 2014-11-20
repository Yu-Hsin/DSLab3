import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class MapperClient { // rename to MapperClient

    /*
     * 2014/11/18 haopingh
     */
    private static final int statusPort = 8080;
    private ServerSocket socketStatus;
    private boolean isIdle = true;

    private ObjectInputStream inputStream;
    private ServerSocket socketclient;
    private static int port2client = 8000;
    private String mapperClass = "TestMapper", mapperFunction = "map"; // mapperClass
    // = run
    private int numReducer = 5;
    private String localFnName = "";
    private Socket toMasterSocket;

    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    socketclient = new ServerSocket(port2client);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void getInitialInfo() {
	try {
	    Socket socket = socketclient.accept();
	    
	    System.out.println("Getting initial information for current task...");
	    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
	    
	    Object obj = ois.readObject();
	    if (obj instanceof MapReduceTask) {
		MapReduceTask mTask = (MapReduceTask)obj;
		numReducer = mTask.getReducerNum();
		mapperClass = mTask.getMapperClass();
		mapperFunction = mTask.getMapperFunc();
	    }
	    
	    ois.close();
	    socket.close();
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }

    /**
     * download the file from the master node
     */
    public void downloadFile() {
	try {
	    Socket socket = socketclient.accept();
	    inputStream = new ObjectInputStream(socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    inputStream));
	    String str = "";

	    str = br.readLine(); // read the number of the split file
	    String fnName = String.format("%04d", Integer.parseInt(str));
	    BufferedWriter bw = new BufferedWriter(new FileWriter("Part-"
		    + fnName));
	    localFnName = "Part-" + fnName;
	    while ((str = br.readLine()) != null) {
		// System.out.println(str);
		bw.write(str);
		bw.write("\n");
	    }
	    bw.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * download the executable file from the master node
     */
    public void downloadExec() {
	try {
	    toMasterSocket = socketclient.accept();
	    InputStream in = toMasterSocket.getInputStream();
	    DataInputStream dis = new DataInputStream(in);
	    String fileName = dis.readUTF();
	    FileOutputStream os = new FileOutputStream(fileName);

	    byte[] byteArray = new byte[1024];
	    int byteRead = 0;

	    while ((byteRead = dis.read(byteArray, 0, byteArray.length)) != -1)
		os.write(byteArray, 0, byteRead);

	    os.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void execute() {

	Process pro;
	try {
	    pro = Runtime.getRuntime().exec("javac " + mapperClass + ".java"); // compile
	    pro.waitFor();
	    Class<?> myClass = Class.forName(mapperClass);
	    Class<?>[] paramsClass = new Class<?>[2];
	    paramsClass[0] = String.class;
	    paramsClass[1] = Output.class;
	    Constructor<?> myCons = myClass.getConstructor();
	    Object object = myCons.newInstance();
	    Method method = null;
	    Output output = new Output(numReducer, "MAPPER");
	    method = object.getClass().getMethod(mapperFunction, paramsClass);

	    BufferedReader br = new BufferedReader(new FileReader(localFnName));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		method.invoke(object, str, output);
	    }
	    br.close();
	    output.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void ackMaster() {
	try {

	    DataOutputStream dout = new DataOutputStream(
		    toMasterSocket.getOutputStream());
	    dout.writeUTF("OK");
	    dout.flush();
	    dout.close();
	    toMasterSocket.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void distribute() {
	//SEND FILE
	ackMaster();
    }

    /**
     *  2014/11/18 haopingh
     */
    public void statusReportThread() {
	try {
	    socketStatus = new ServerSocket(statusPort);
	    Thread t = new Thread(new StatusReportThread(socketStatus));
	    t.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private class StatusReportThread implements Runnable {
	ServerSocket mServer = null;
	
	public StatusReportThread(ServerSocket s) {
	    mServer = s;
	}

	@Override
	public void run() {
	    while(true) {
		try {
		    Socket request = mServer.accept();
		    
		    DataInputStream dis = new DataInputStream(request.getInputStream());
		    DataOutputStream dos = new DataOutputStream(request.getOutputStream());
		    String s = dis.readUTF();
		    
		    if (s.equals("status")) dos.writeUTF(isIdle ? "idle" : "busy");
		    dos.flush();
		    request.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	    
	}
    }
    
    public static void main(String[] args) {
	MapperClient client = new MapperClient();


	// TODO client.loadConfig(args[0]); // load configuration file
	client.openSocket(); // create a socket for listenting to the master
	
	/* Start another server to respond the status request */
	client.statusReportThread();
	
	// node
	// communication (get reducer ip, master ip)
	client.getInitialInfo();

	client.downloadFile(); // download the split file from the master node
	client.downloadExec(); // download the jar file from the master node
	client.execute(); // execute the jar file, generate intermediate file
	client.distribute(); // send same keys to same reducers
	// master

    }

}

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import Utils.Utility;

public class MapperClient {

    private static int portToMaster;
    private ServerSocket socketToMaster;

    private static int statusPort;
    private ServerSocket socketStatus;

    private MapReduceTask mTask;
    private static Status status;
    private String mapperClass, mapperFunction;
    private int numReducer;
    private String[] reducerIP, backReducerIP;
    private int[] reducerPort, backReducerPort;

    private String jobID;
    private String localFnName = "";

    private Socket toMasterSocket;

    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    socketToMaster = new ServerSocket(portToMaster);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void setTask(MapReduceTask m) {
	backReducerIP = mTask.getBackReducerIP();
	backReducerPort = mTask.getBackReducerPortToMapper();
	numReducer = mTask.getReducerNum();
	mapperClass = mTask.getMapperClass();
	mapperFunction = mTask.getMapperFunc();
	reducerIP = mTask.getReducerIP();
	jobID = mTask.getJobID();
	reducerPort = mTask.getReducerPortToMapper();
    }

    public void getInitialInfo() {
	try {
	    Socket socket = socketToMaster.accept();

	    System.out
		    .println("Getting initial information for current task...");
	    ObjectInputStream ois = new ObjectInputStream(
		    socket.getInputStream());

	    Object obj = ois.readObject();
	    if (obj instanceof MapReduceTask)
		mTask = (MapReduceTask) obj;
	    setTask(mTask);
	    ois.close();
	    socket.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * download the file from the master node
     */
    public void downloadFile() {
	try {
	    Socket socket = socketToMaster.accept();
	    ObjectInputStream inputStream = new ObjectInputStream(
		    socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    inputStream));
	    String str = "";

	    str = br.readLine(); // read the number of the split file
	    String fnName = String.format("%04d", Integer.parseInt(str));
	    BufferedWriter bw = new BufferedWriter(new FileWriter(jobID
		    + "_Part-" + fnName));
	    localFnName = jobID + "_Part-" + fnName;
	    while ((str = br.readLine()) != null) {
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
	    toMasterSocket = socketToMaster.accept();
	    Utility.downloadExec(toMasterSocket);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

    /**
     * Run the mapper function
     */
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
	    Output output = new Output(numReducer, jobID + "_mapper");
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

    /**
     * Send the message to master telling it finishes sending the files to the
     * reducers
     */
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
	// send files to the reducer
	sendToReducer(reducerIP, reducerPort);
	// send files to the back up reducer
	sendToReducer(backReducerIP, backReducerPort);
	ackMaster();
    }

    /**
     * 
     * @param inputIP
     *            a list of reducer's IP
     * @param inputPort
     *            a list of reducer's port number
     */
    public void sendToReducer(String[] inputIP, int[] inputPort) {
	for (int i = 0; i < inputIP.length; i++) {
	    try {
		Socket socket = new Socket(inputIP[i], inputPort[i]);
		OutputStreamWriter dOut = new OutputStreamWriter(
			socket.getOutputStream());
		BufferedReader br = new BufferedReader(new FileReader(jobID
			+ "_mapper" + i));
		String str = "";
		while ((str = br.readLine()) != null) {
		    dOut.write(str + "\n");
		    dOut.flush();
		}
		br.close();
		dOut.close();
		socket.close();
	    } catch (UnknownHostException e) {
		e.printStackTrace();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }

    public void statusReportThread() {
	try {
	    socketStatus = new ServerSocket(statusPort);
	    status = new Status(socketStatus);
	    Thread t = new Thread(status);
	    t.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void waitForMaster() {
	try {

	    Socket s = socketToMaster.accept();
	    System.out.println("Connect to master!");
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    s.getInputStream()));

	    String str = br.readLine();
	    System.out.println(str + "|");
	    if (Integer.parseInt(str) == 0) {
		return;
	    } else if (Integer.parseInt(str) == 1) { // error handling
		status.setStatus(false);
		//The code commented below are used for testing error handling for mapper
		
		try {
		    System.out.println("[Mapper] Force sleep");
		    Thread.sleep(10000);
		    System.out.println("Wake up");
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
		
		this.downloadFile(); // download the split file from the master
		this.downloadExec(); // download the java file from the master
		this.execute(); // execute the java file, generate intermediate files
		this.distribute(); // send same keys to same reducers
		System.out.println("[Mapper] Status: finish execution");
	    }

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) {
	portToMaster = Integer.parseInt(args[0]);
	statusPort = Integer.parseInt(args[1]);
	MapperClient client = new MapperClient();
	client.openSocket(); // create a socket for listenting to the master
	client.statusReportThread(); // start another server to respond the
				     // status request
	while (true) {
	    status.setStatus(true);
	    System.out.println("[Mapper] Status: idle, wating for execution");
	    client.getInitialInfo();
	    System.out.println("[Mapper] Status: busy, start executing");
	    
	    client.waitForMaster();
	    
	    /*
	    client.downloadFile(); // download the split file from the master
	    client.downloadExec(); // download the java file from the master
	    client.execute(); // execute the java file, generate intermediate files
	    client.distribute(); // send same keys to same reducers
	    System.out.println("[Mapper] Status: finish execution");
	    */
	}
    }

}

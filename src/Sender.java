import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class Sender implements Runnable {

    Socket socket;
    String fnName, reducer;
    int port2reducer;

    public Sender(Socket socket, String fnName, String reducer, int port) {
	this.socket = socket;
	this.fnName = fnName;
	this.reducer = reducer;
	port2reducer = port;
    }

    @Override
    public void run() {
	try {
	    Socket socket = new Socket(reducer, port2reducer);

	    OutputStreamWriter dOut = new OutputStreamWriter(
		    socket.getOutputStream());
	    BufferedReader br = new BufferedReader(new FileReader(fnName));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		dOut.write(str);
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

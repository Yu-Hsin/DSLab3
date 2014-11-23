
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Status implements Runnable {
    
    private boolean isIdle = true;
    ServerSocket mServer = null;

    public Status(ServerSocket s) {
	mServer = s;
    }
    
    public void setStatus (boolean b) {
	isIdle = b;
    }
    
    @Override
    public void run() {
	while (true) {
	    try {
		Socket request = mServer.accept();

		DataOutputStream dos = new DataOutputStream(
			request.getOutputStream());

		dos.writeUTF(isIdle? "idle" : "busy");
		dos.flush();
		request.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}

    }

}

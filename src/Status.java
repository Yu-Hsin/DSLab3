import java.io.DataInputStream;
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

    @Override
    public void run() {
	while (true) {
	    try {
		Socket request = mServer.accept();
		DataInputStream dis = new DataInputStream(
			request.getInputStream());
		DataOutputStream dos = new DataOutputStream(
			request.getOutputStream());
		String s = dis.readUTF();

		if (s.equals("status"))
		    dos.writeUTF(isIdle? "idle" : "busy");
		dos.flush();
		request.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}

    }

}

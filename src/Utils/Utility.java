package Utils;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class Utility {
    public static void downloadExec(Socket socket) {
	try {
	    InputStream in = socket.getInputStream();
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
}

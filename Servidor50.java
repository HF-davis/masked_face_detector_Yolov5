import java.util.Scanner;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.ServerSocket;


import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.net.Socket;

public class Servidor50 {
   
    public static class TCPServer50 {
    private String message;
    
    int nrcli = 0;

    public static final int SERVERPORT = 9999;
    private OnMessageReceived messageListener = null;
    private boolean running = false;
    TCPServerThread50[] sendclis = new TCPServerThread50[10];

    PrintWriter mOut;
    BufferedReader in;
    
    ServerSocket serverSocket;

    //el constructor pide una interface OnMessageReceived
    public TCPServer50(OnMessageReceived messageListener) {
        this.messageListener = messageListener;
    }
    
    public OnMessageReceived getMessageListener(){
        return this.messageListener;
    }
    
    public void sendMessageTCPServer(String message){
        for (int i = 1; i <= nrcli; i++) {
            sendclis[i].sendMessage(message);
            System.out.println("ENVIANDO A JUGADOR " + (i));
        }
    }
    

    
    public void run(){
        running = true;
        try{
            System.out.println("TCP Server"+"S : Connecting...");
            serverSocket = new ServerSocket(SERVERPORT);
            
            while(running){
                Socket client = serverSocket.accept();
                System.out.println("TCP Server"+"S: Receiving...");
                nrcli++;
                System.out.println("Engendrado " + nrcli);
                sendclis[nrcli] = new TCPServerThread50(client,this,nrcli,sendclis);
                Thread t = new Thread(sendclis[nrcli]);
                t.start();
                System.out.println("Nuevo conectado:"+ nrcli+" jugadores conectados");
                
            }
            
        }catch( Exception e){
            System.out.println("Error"+e.getMessage());
        }finally{

        }
    }
    public  TCPServerThread50[] getClients(){
        return sendclis;
    } 

    public interface OnMessageReceived {
        public void messageReceived(String message);
    }
}
    
    public static class TCPServerThread50 extends Thread{
    
    private Socket client;
    private TCPServer50 tcpserver;
    private int clientID;                 
    private boolean running = false;
    public PrintWriter mOut;
    public BufferedReader in;
    private TCPServer50.OnMessageReceived messageListener = null;
    private String message;
    TCPServerThread50[] cli_amigos;

    public TCPServerThread50(Socket client_, TCPServer50 tcpserver_, int clientID_,TCPServerThread50[] cli_ami_) {
        this.client = client_;
        this.tcpserver = tcpserver_;
        this.clientID = clientID_;
        this.cli_amigos = cli_ami_;
    }
    
     public void trabajen(int cli){      
         mOut.println("TRABAJAMOS ["+cli+"]...");
    }
    
    public void run() {
        running = true;
        try {
            try {               
                boolean soycontador = false;                
                mOut = new PrintWriter(new BufferedWriter(new OutputStreamWriter(client.getOutputStream())), true);
                System.out.println("TCP Server"+ "C: Sent.");
                messageListener = tcpserver.getMessageListener();
                in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                while (running) {
                    message = in.readLine();
                    
                    if (message != null && messageListener != null) {
                        messageListener.messageReceived(message);
                    }
                    

                    message = null;
                }
                System.out.println("RESPONSE FROM CLIENT"+ "S: Received Message: '" + message + "'");
            } catch (Exception e) {
                System.out.println("TCP Server"+ "S: Error"+ e);
            } finally {
                client.close();
            }

        } catch (Exception e) {
            System.out.println("TCP Server"+ "C: Error"+ e);
        }
    }
    
    public void stopClient(){
        running = false;
    }
    
    public void sendMessage(String message){//funcion de trabajo
        if (mOut != null && !mOut.checkError()) {
            mOut.println( message);
            mOut.flush();
        }
    }
    
}
   TCPServer50 mTcpServer;
   Scanner sc;
   
   
   public static void main(String[] args) {
       Servidor50 objser = new Servidor50();
       objser.iniciar();
   }
   void iniciar(){
       new Thread(
            new Runnable() {

                @Override
                public void run() {
                      mTcpServer = new TCPServer50(
                        new TCPServer50.OnMessageReceived(){
                            @Override
                            public void messageReceived(String message){
                                ServidorRecibe(message);
                            }
                        }
                    );
                    mTcpServer.run();                   
                }
            }
        ).start();
        //-----------------
        String salir = "n";
        sc = new Scanner(System.in);
        System.out.println("Servidor bandera 01");
        while( !salir.equals("s")){
            salir = sc.nextLine();
            ServidorEnvia(salir);
       }
       System.out.println("Servidor bandera 02"); 
   
   }
   void ServidorRecibe(String llego){
       System.out.println("SERVIDOR40 El mensaje:" + llego);
   }
   void ServidorEnvia(String envia){
        if (mTcpServer != null) {
            mTcpServer.sendMessageTCPServer(envia);
        }
   }
}

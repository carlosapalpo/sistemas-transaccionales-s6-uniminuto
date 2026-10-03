package co.edu.uniminuto.transacciones;
public final class Main {
 public static void main(String[] args)throws Exception {
  String option=args.length==0?"menu":args[0];
  if(option.equals("menu")){System.out.println("SEMANA 6 | 1 ActiveMQ | 2 Cache | 3 Storm | 4 2PC | 5 Todos | 0 Salir");option=new java.util.Scanner(System.in).nextLine().trim();}
  switch(option){case "1":case "messaging":MessagingDemo.run();break;case "2":case "cache":CacheDemo.run();break;case "3":case "storm":RealtimeDemo.run();break;case "4":case "2pc":TwoPhaseCommitDemo.run();break;case "recover":TwoPhaseCommitDemo.recover();break;case "5":case "all":MessagingDemo.run();CacheDemo.run();RealtimeDemo.run();TwoPhaseCommitDemo.run();break;case "0":break;default:throw new IllegalArgumentException("Opcion desconocida: "+option);}
  // La demostracion se lanza en su propia JVM. Cierra los hilos auxiliares de Storm.
  System.exit(0);
 }
}

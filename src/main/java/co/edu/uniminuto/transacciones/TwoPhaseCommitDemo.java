package co.edu.uniminuto.transacciones;
import java.sql.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.util.*;
public final class TwoPhaseCommitDemo {
 private static final Path DIR=Path.of("data","coordinador");
 private static String xid(String tx,int branch){return "'"+tx+"','rama"+branch+"',6";}
 private static void sql(Connection c,String command)throws SQLException{try(Statement s=c.createStatement()){s.execute(command);}}
 private static void journal(String tx,String state)throws Exception {
  Files.createDirectories(DIR);
  try(FileChannel f=FileChannel.open(DIR.resolve(tx+".log"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.APPEND)) {
   ByteBuffer b=ByteBuffer.wrap((state+"\n").getBytes(StandardCharsets.UTF_8));while(b.hasRemaining())f.write(b);f.force(true);
  }
 }
 private static BigDecimal balance(Connection c,long id)throws SQLException {
  try(PreparedStatement p=c.prepareStatement("SELECT saldo FROM cuentas WHERE id=?")){p.setLong(1,id);try(ResultSet r=p.executeQuery()){if(!r.next())throw new SQLException("Cuenta inexistente");return r.getBigDecimal(1);}}
 }
 private static void delta(Connection c,long id,BigDecimal amount)throws SQLException {
  try(PreparedStatement p=c.prepareStatement("UPDATE cuentas SET saldo=saldo+? WHERE id=? AND saldo+?>=0")){p.setBigDecimal(1,amount);p.setLong(2,id);p.setBigDecimal(3,amount);if(p.executeUpdate()!=1)throw new SQLException("Saldo insuficiente o cuenta inexistente");}
 }
 private static void transaction(boolean reject)throws Exception {
  String tx="s6"+UUID.randomUUID().toString().replace("-","");Connection[] cs={DbConfig.open(),DbConfig.second()};
  boolean[] started=new boolean[2],ended=new boolean[2];boolean decision=false;
  journal(tx,"ROLLBACK");
  try {
   for(int i=0;i<2;i++){sql(cs[i],"XA START "+xid(tx,i));started[i]=true;}
   delta(cs[0],1,new BigDecimal("-150"));delta(cs[1],2,new BigDecimal("150"));
   for(int i=0;i<2;i++){sql(cs[i],"XA END "+xid(tx,i));ended[i]=true;}
   sql(cs[0],"XA PREPARE "+xid(tx,0));System.out.println("2PC PREPARE rama0: SI");
   if(reject)throw new SQLException("Voto NO controlado antes de preparar rama1");
   sql(cs[1],"XA PREPARE "+xid(tx,1));System.out.println("2PC PREPARE rama1: SI");
   journal(tx,"COMMIT");decision=true;System.out.println("2PC DECISION COMMIT persistida");
   for(int i=0;i<2;i++)sql(cs[i],"XA COMMIT "+xid(tx,i));journal(tx,"DONE");System.out.println("2PC COMMIT ambas ramas");
  } catch(Exception e) {
   if(decision){System.out.println("2PC COMMIT pendiente: ejecutar recover; nunca cambiar a ROLLBACK");throw e;}
   boolean rolledBack=true;
   for(int i=0;i<2;i++)if(started[i])try {if(!ended[i])sql(cs[i],"XA END "+xid(tx,i));sql(cs[i],"XA ROLLBACK "+xid(tx,i));}catch(SQLException cleanup){rolledBack=false;e.addSuppressed(cleanup);}
   if(rolledBack)journal(tx,"DONE");
   System.out.println("2PC ROLLBACK: "+e.getMessage());if(!reject||!rolledBack)throw e;
  } finally {for(Connection c:cs)c.close();}
 }
 public static void recover()throws Exception {
  if(!Files.exists(DIR)){System.out.println("2PC sin diario pendiente");return;}
  try(var paths=Files.list(DIR)) {
   for(Path f:paths.filter(p->p.toString().endsWith(".log")).toList()) {
    List<String> lines=Files.readAllLines(f);if(lines.contains("DONE"))continue;
    String tx=f.getFileName().toString().replace(".log","");if(!tx.matches("s6[0-9a-f]{32}"))throw new IllegalStateException("Identificador de diario invalido");
    String action=lines.contains("COMMIT")?"COMMIT":"ROLLBACK";
    try(Connection a=DbConfig.open();Connection b=DbConfig.second()) {
     Connection[] cs={a,b};for(int i=0;i<2;i++)try{sql(cs[i],"XA "+action+" "+xid(tx,i));}catch(SQLException e){if(e.getErrorCode()!=1397)throw e;}
    }
    journal(tx,"DONE");System.out.println("2PC RECUPERADO "+tx+" decision="+action);
   }
  }
 }
 public static void run()throws Exception {
  recover();BigDecimal initial;
  try(Connection a=DbConfig.open();Connection b=DbConfig.second()){initial=balance(a,1).add(balance(b,2));System.out.println("2PC ANTES origen="+balance(a,1)+" destino="+balance(b,2));}
  transaction(false);BigDecimal afterA,afterB;
  try(Connection a=DbConfig.open();Connection b=DbConfig.second()){afterA=balance(a,1);afterB=balance(b,2);DbConfig.check(afterA.add(afterB).compareTo(initial)==0,"No se conserva el total");System.out.println("2PC DESPUES COMMIT origen="+afterA+" destino="+afterB);}
  transaction(true);
  try(Connection a=DbConfig.open();Connection b=DbConfig.second()){DbConfig.check(balance(a,1).equals(afterA)&&balance(b,2).equals(afterB),"Rollback cambio los saldos");System.out.println("2PC DESPUES ROLLBACK origen="+balance(a,1)+" destino="+balance(b,2));}
 }
}

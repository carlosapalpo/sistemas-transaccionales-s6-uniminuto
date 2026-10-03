package co.edu.uniminuto.transacciones;
import java.sql.*;
public final class DbConfig {
 private DbConfig() {}
 public static Connection open() throws SQLException {return connect("DB_URL","sistemas_transaccionales_s6");}
 public static Connection second() throws SQLException {return connect("DB_SECOND_URL","sistemas_transaccionales_s6_destino");}
 private static Connection connect(String variable,String schema) throws SQLException {
  String password=System.getenv("DB_PASSWORD");
  if(password==null) throw new IllegalStateException("Configure DB_PASSWORD en la terminal o en el IDE.");
  String url=System.getenv().getOrDefault(variable,"jdbc:mysql://localhost:3306/"+schema+"?serverTimezone=America/Bogota&useSSL=false&allowPublicKeyRetrieval=true");
  return DriverManager.getConnection(url,System.getenv().getOrDefault("DB_USER","root"),password);
 }
 public static void event(String id,String exercise,String detail) throws SQLException {
  try(Connection c=open();PreparedStatement p=c.prepareStatement("INSERT IGNORE INTO eventos(id,ejercicio,detalle) VALUES(?,?,?)")) {
   p.setString(1,id);p.setString(2,exercise);p.setString(3,detail);p.executeUpdate();
  }
 }
 public static int stock() throws SQLException {
  try(Connection c=open();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT existencias FROM productos WHERE id=1")) {if(!r.next())throw new SQLException("Producto inexistente");return r.getInt(1);}
 }
 public static void check(boolean ok,String message) {if(!ok)throw new IllegalStateException(message);}
}

import java.io.*
import java.lang.Exception
import java.lang.NumberFormatException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

fun main(args: Array<String>) {
    try{
        //abrir conexion
        ConexionColegio.conectar("colegio", "root", "foca")

        var opction= 1
        while (opction != 0) {
            try {
                println(menu())
                print("--> ")
                opction = readln().toInt()

                when (opction) {
                    1 -> ejecutarScriptSQL()
                    2 -> buscaAlumno()
                    3 -> crearAlumnoBD()
                    4 -> modificarAlumno()
                    5 -> eliminarAlumno()
                    0 -> println("exit")
                    else -> println("opción desconocida")
                }
            }
            catch (ex: NumberFormatException) { println("Inserta solo números") }
            catch (ex: SQLException) { println("error sql") }
            catch (ex: Exception){ println(ex.message) }
        }
        ConexionColegio.cerrarConexion()
    }
    catch (ex: Exception){ println(ex.message) }

}
fun menu(): String{
    return """
        (1) Ejecutar Script SQL en MariaDB para crear tablas en BD 'colegio'
        (2) Ejecuta sentencia SQL 'SELECT' preparada en MariaDB
        (3) Ejecutar sentencia SQL 'INSERT' preparada en MariaDB
        (4) Ejecutar sentencia SQL 'UPDATE' preparada en MariaDB
        (5) Ejecutar sentencia SQL 'UPDATE' preparada en MariaDB
        -------------------------------------------------------
        (0) salir
    """.trimIndent()
}
fun leerScript(file: File): String {
    val script = File(file.path)
    return script.readText()
    //devuelve un string con el texto del fichero
}
fun busarFile(): File{
    print("Ruta del fichero --> ")
    val rutaArchivo = readln();
    val archivo = File(rutaArchivo);

    //comprobamos si existe el archivo y si es un fichero
    if (archivo.exists()){
        if(archivo.isFile){
            return archivo
        }
        else throw NoEsUnFicheroException("El archivo no es un fichero")
    }
    else throw ArchivoNoExisteException("No se ha encontrado el fichero")
}
fun ejecutarScriptSQL(){
    val script = leerScript(busarFile()) //script: String del fichero
    ConexionColegio.ejecutarScript(script)
}
fun buscaAlumno(){
    mostrarAlumnosBD()
    mostrarAlumnoDNI(pedirDNI())
}
fun mostrarAlumnoDNI(dni: String){
    ConexionColegio.ejecutarScript("select * from alumnos where dni = $dni")
}
fun modificarAlumno(){
    mostrarAlumnosBD()
    val dni = pedirDNI() //dni
    val columna = pedirColumna() //nombre columna
    print("Nuevo ${columna.uppercase()}: ") //nueva entrada de la columna

    ConexionColegio.actualizarAlumno(dni, columna, readln())
    println("alumno modificado conrrectamente")
}
fun eliminarAlumno(){
    val dni = pedirDNI() //dni

    ConexionColegio.deleteAlumno(dni)
    println("alumno eliminado correctamente")
}
fun pedirColumna(): String{
    print("Nombre columna: ")
    val columna = readln() //nombre columna

    if (ConexionColegio.nombreColumnaPerteneceATablaAlumnos(columna)) return columna
    else throw ColumnaNoEncontradaException("No se encontrado la columna")
}
fun pedirDNI(): String{
    print("DNI: ")
    val dni = readln(); //dni

    //si dni no está en la bdd salta un error
    if (!ConexionColegio.comprobarDNI(dni)) throw DNINoEncontradoException("No existe un alumno con ese dni")
    else return dni;
}
fun mostrarAlumnosBD(){
    ConexionColegio.ejecutarScript("SELECT * FROM alumnos")
}
fun crearAlumnoBD(){
    ConexionColegio.insertarAlumno(pedirAlumno())
    println("alumno creado correctamente")
}
fun pedirAlumno(): Alumno {
    print("Introduce el DNI: ")
    val dni = readln() //dni
    if(ConexionColegio.comprobarDNI(dni)) throw DNIYaExisteException("El DNI ya existe en la base de datos")

    print("Introduce el apellido y nombre (APENOM): ")
    val apenom = readln() //nombre y apellido

    print("Introduce la dirección (DIREC): ")
    val direc = readln() //dirección

    print("Introduce la población (POBLA): ")
    val pobla = readln() //población

    print("Introduce el teléfono (TELEF): ")
    val telef = readln() //teléfono

    return Alumno(dni, apenom, direc, pobla, telef)
}
object ConexionColegio{
    private lateinit var connection: Connection;
    fun conectar(nameBD:String, user: String, pass: String){
        try{
            //abrimos conexión con url, nombre de usuario, y contraseña proporcionada
            connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/$nameBD", user, pass)
        }catch (ex: Exception){
            throw AbrirConexionException("Error al conectarse con la base de datos")
        }
    }
    fun cerrarConexion(){
        try{
            connection.close()
        }catch(e: Exception){
            throw CerrarConexionException("Error al cerrar la base de datos")
        }
    }

    fun comprobarDNI(dni: String):Boolean{
        connection.prepareStatement("select dni from alumnos").use {pst ->
            pst.executeQuery().use { rs ->

                //comprueba para cada fila si el dni es igual que el proporcionado
                while (rs.next()){
                    if (dni == rs.getString(1)) return true;
                }
            }
        }
        return false;
    }
    fun actualizarAlumno(dni: String, columna: String, mod: String){
        connection.prepareStatement("UPDATE alumnos SET $columna = ? where dni = ?").use{pst ->
            pst.setString(1, mod)
            pst.setString(2, dni)
            pst.executeUpdate()
        }
    }
    fun insertarAlumno(alumno: Alumno){
        connection.prepareStatement("insert into alumnos values(?, ?, ?, ?, ?)").use { pst ->
            pst.setString(1, alumno.dni)
            pst.setString(2, alumno.apenom)
            pst.setString(3, alumno.direc)
            pst.setString(4, alumno.pobla)
            pst.setString(5, alumno.telef)

            pst.executeUpdate()
        }
    }
    fun deleteAlumno(dni: String){
        connection.prepareStatement("Delete from alumnos where dni = ?").use{pst ->
            pst.setString(1, dni)
            pst.executeUpdate()
        }
    }
    fun nombreColumnaPerteneceATablaAlumnos(nombreColumna: String): Boolean{
        connection.prepareStatement("select * from alumnos limit 1").use{pst ->
            pst.executeQuery().use {rs ->
                rs.next()
                val numeroColumnas = rs.metaData.columnCount;
                for (i in 1..numeroColumnas){
                    //comprueba en cada iteración si el nombre de la columna es igual que la proporcionada
                    if (nombreColumna.equals(rs.metaData.getColumnName(i), true)) return true
                }
            }
        }
        return false
    }
    fun ejecutarScript(query: String){
        val pst = connection.createStatement()
        pst.use {
            //si execute ha hecho un select devuelve true
            //si ha se ha hecho un select..
            if(pst.execute(query)){

                pst.executeQuery(query).use{
                    while(it.next()){
                        for(i in 1..it.metaData.columnCount){
                            //imprimimos cada una de las columnas de las filas
                            println(it.metaData.getColumnName(i) + " --> " + it.getString(i))
                        }
                        println()
                    }
                }
            }
        }
    }

}
class AbrirConexionException(message: String): Exception(message)
class CerrarConexionException(message: String): Exception(message)
class DNINoEncontradoException(message: String): Exception(message)
class NoEsUnFicheroException(message: String): Exception(message)
class ArchivoNoExisteException(message: String): Exception(message)
class DNIYaExisteException(message: String): Exception(message)
class ColumnaNoEncontradaException(message: String): Exception(message)
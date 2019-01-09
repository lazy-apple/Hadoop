package MySQL;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author LaZY（李志一）
 * @data 2019/1/9 - 18:59
 */
public class MyDBWritable implements DBWritable,Writable{
    //数据库字段
    public int id;
    public String name;
    public String txt;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(txt);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        name = dataInput.readUTF();
        txt = dataInput.readUTF();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,id);
        preparedStatement.setString(2,name);
        preparedStatement.setString(3,txt);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);
        txt = resultSet.getString(3);

    }
}

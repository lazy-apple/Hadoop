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
    //数据库words表字段
    public int id = 0;
    public String name = "";
    public String txt = "";
    //数据库stats表字段
    private String word = "";
    private int wordCount = 0;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getWordCount() {
        return wordCount;
    }

    public void setWordCount(int wordCount) {
        this.wordCount = wordCount;
    }

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
        dataOutput.writeUTF(word);
        dataOutput.writeInt(wordCount);
        }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        name = dataInput.readUTF();
        txt = dataInput.readUTF();
        word = dataInput.readUTF();
        wordCount = dataInput.readInt();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,word);
        preparedStatement.setInt(2,wordCount);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);
        txt = resultSet.getString(3);
    }
}

package ru.ifmo.escience.ignite.week4;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;

import javax.cache.Cache;
import java.io.IOException;
import java.util.List;

import static ru.ifmo.escience.ignite.Utils.print;
import static ru.ifmo.escience.ignite.Utils.readln;

public class IgniteSqlStart {
    public static void main(String[] args) throws IOException {
        //System.setProperty("IGNITE_H2_DEBUG_CONSOLE", "true");

        try (Ignite node = Ignition.start("Week4/config/default.xml")) {
            //node.cache("mycache").put(1, new Person("John", "Doe", 5000));
            //node.cache("mycache").put(2, new Person("John", "Shepard", 1100));

            //IgniteCache<Integer, Person> cache =node.cache ("mycache");
            //cache.query(new SqlFieldsQuery("insert into Person (_Key,name,surname,salary) values (3,'John','Johnsonn',1200)"));
            //SqlQuery sql=new SqlQuery(Person.class, "salary>?");
            //try (QueryCursor<Cache.Entry<Integer,Person>> cursor=cache.query(sql.setArgs(1000)))
            //{
            //    for (Cache.Entry<Integer,Person> e: cursor)
            //    {
            //        System.out.println(e.getValue().toString());
            //    }
            //}

            //SqlFieldsQuery sql =new SqlFieldsQuery
            //        ("select concat(name,' ',surname) from person where salary > ?");

            IgniteCache dflt=node.getOrCreateCache("default");
                    myquery(dflt,"create table \"PUBLIC\".Library(isbn integer,name varchar,author varchar,genre varchar,reader_id integer,primary key(isbn,reader_id))" +
                            "WITH \"cache_name=Library,key_type=Book_id,value_type=Book\"");
                    myquery(dflt,"insert into Library(isbn,name,author,genre,reader_id) values (3215648,'test1','test2','test3',54654168)");


            BinaryObjectBuilder bldr =node.binary().builder("Book_id");
            bldr.setField("ISBN",3215648 );
            bldr.setField("READER_ID",54654168 );
            BinaryObject key=bldr.build();
            print(node.cache("Library").withKeepBinary().get(key).toString());

            BinaryObjectBuilder bldr1 =node.binary().builder("Book_id");
            bldr1.setField("ISBN",1 );
            bldr1.setField("READER_ID",1 );

            BinaryObjectBuilder bldr2=node.binary().builder("Book");
            bldr2.setField("NAME","val1");
            bldr2.setField("AUTHOR","val2");
            bldr2.setField("GENRE","val3");

            node.cache("Library").withKeepBinary().put(bldr1.build(),bldr2.build());
                print(myquery(dflt,"select * from Library").toString());




            //try (FieldsQueryCursor<List<?>> cursor=cache.query(sql.setArgs(1000)))
            //{
            //    for (List<?> e :cursor)
            //    {
            //        System.out.println(e);
            //    }
            //}
            //Person p= cache.get(3);
            //print (p.toString());
            readln();
        }

    }
    public static List<List> myquery(IgniteCache dflt,String mystring){
        return dflt.query(new SqlFieldsQuery(mystring).setSchema("PUBLIC")).getAll();
    }

}

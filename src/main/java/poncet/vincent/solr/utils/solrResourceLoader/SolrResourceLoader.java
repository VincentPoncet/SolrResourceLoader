package poncet.vincent.solr.utils.solrResourceLoader;

/**
 * Created by vincentponcet on 04/07/2016.
 */

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class SolrResourceLoader
{
    public static void main(String[] args)
    {

        String host = "";
        String username = "";
        String password = "";
        String resourceFilePath = "";
        String coreName = "";
        String resourceName = "";

        if (!( (args.length == 4) || (args.length == 6) ))
        {
            System.out.println ("Usage : host resourceFilePath coreName resourceName [username password]");
        } else
        {
            host = args[0];
            resourceFilePath = args[1];
            coreName = args[2];
            resourceName = args[3];
            if ( args.length == 6 )
            {
                username = args[4];
                password = args[5];
            }

            String resourceQuery = "insert into solr_admin.solr_resources (core_name, resource_name, resource_value) values (?, ?, ?)";
            try (Cluster cluster = Cluster.builder().withCredentials(username, password).addContactPoint(host).build())
            {
                byte[] readFileBytes = Files.readAllBytes((new File(resourceFilePath)).toPath());
                Session javaDriverSession = cluster.connect();
                BoundStatement bstmt = new BoundStatement(javaDriverSession.prepare(resourceQuery));
                bstmt.bind(coreName, resourceName, ByteBuffer.wrap(readFileBytes));
                bstmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
                javaDriverSession.execute(bstmt);
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }
}
package org.apache.cassandra.stress.operations;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientSyncer extends Operation {
    private PrintStream output;
    public int getKeyForClient(int i) {
        return session.getNumDifferentKeys() + i + 10;
    }
    public ClientSyncer(Session client, int idx, PrintStream out) {
        super(client, idx);
        output = out;
    }

    @Override
    public void run(Cassandra.Client client) throws IOException {
        throw new RuntimeException("Experiment10 must be run with COPS client");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException {

        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        Column c = new Column(columnName(0, session.timeUUIDComparator)).setValue(ByteBufferUtil.bytes(1)).setTimestamp(FBUtilities.timestampMicros());
        ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
        mutations.add(new Mutation().setColumn_or_supercolumn(column));
        mutationMap.put("Standard1", mutations);

        int thisClientKey = getKeyForClient(session.stressIndex);
        String format = "%0" + session.getTotalKeysLength() + "d";
        String rawKey = String.format(format, thisClientKey);

        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        record.put(ByteBufferUtil.bytes(rawKey), mutationMap);

        boolean success = true;
        String exceptionMessage = null;
        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;
            try
            {
                clientLibrary.getContext().clearDeps();
                clientLibrary.batch_mutate(record);
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Error inserting unique key %s %s%n",
                    rawKey,
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        output.println("Client "+session.stressIndex+ " ready. Written key "+rawKey);
        //Wait for all clients to start up
        SlicePredicate nColumnsPredicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                false, 1));
        ArrayList<ByteBuffer> keys = new ArrayList<>();
        for(int i = 0; i < session.stressCount; ++i)
            keys.add(ByteBufferUtil.bytes(String.format(format, getKeyForClient(i))));

        ColumnParent parent = new ColumnParent("Standard1");
        int columnCount = 0;
        Map<ByteBuffer, List<ColumnOrSuperColumn>> results;

        success = false;
        exceptionMessage = null;


        for (int t = 0; t < session.getRetryTimes(); ++t) {
            try {
                columnCount = 0;
                String missingKeys = "";
                results = clientLibrary.multiget_slice(keys, parent, nColumnsPredicate);
                for (Map.Entry<ByteBuffer,List<ColumnOrSuperColumn>> kvs : results.entrySet()) {
                    List<ColumnOrSuperColumn> result = kvs.getValue();
                    int size = result.size();
                    columnCount += size;
                    if(size == 0)
                        missingKeys += ByteBufferUtil.string(kvs.getKey());
                }
                success = (columnCount == session.stressCount);
                output.println("Number of clients ready = "+columnCount+"  Missing ="+missingKeys);
                if (success)
                    break;
                Thread.sleep(500);
            } catch (Exception e) {
                exceptionMessage = getExceptionMessage(e);
            }
        }

        if (!success) {
            error(String.format("Wait for clients failed  %s!!!!!",  (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

    }

}

package org.apache.cassandra.stress.operations;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


public class Experiment10 extends Operation {

    private static ZipfianGenerator zipfGen;
    private static ByteBuffer value;
    public Experiment10(Session session, int index) {
        super(session, index);
        if (zipfGen == null) {
            int numKeys = session.getNumDifferentKeys();
            int numServ = session.getNum_servers();
            int keyPerServ = numKeys / numServ;
            zipfGen = new ZipfianGenerator(keyPerServ, session.getZipfianConstant());
        }
    }

    private List<ByteBuffer> generateReadTxnKeys(int numTotalServers, int numInvolvedServers, int keysPerServer) {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        List<Integer> allServerIndices = new ArrayList<Integer>(numTotalServers);
        for (int i = 0; i < numTotalServers; i++) {
            allServerIndices.add(i);
        }

        int srvIndex = Stress.randomizer.nextInt(numTotalServers);

        // choose K keys for each server
        for (int i = 0; i < numInvolvedServers; i++) {
            for (int k = 0; k < keysPerServer; k++) {
                keys.add(getZipfGeneratedKey(srvIndex));
            }
            srvIndex = (srvIndex + 1) % numTotalServers;
        }

        return keys;
    }


    private ByteBuffer generateValue() {
        int valueLen = session.getColumnSize();
        byte[] valueArray = new byte[valueLen];
        Arrays.fill(valueArray, (byte) 'y');
        return ByteBuffer.wrap(valueArray);
    }


    private  ByteBuffer getZipfGeneratedKey(int srvIndex) {

        int index = zipfGen.nextInt();
        ArrayList<ByteBuffer> list = session.generatedKeysByServer.get(srvIndex);
        if (index >= list.size())
            return list.get(list.size() - 1);
        else
            return list.get(index);
    }

    @Override
    public void run(Cassandra.Client client) throws IOException {
        throw new RuntimeException("Experiment10 must be run with COPS client");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException {
        //do all random tosses here
        while(zipfGen == null); // wait until initialization is over
        double target_p_w = session.getWrite_fraction();
        int partitionsToReadFrom = session.getKeys_per_read();
        assert partitionsToReadFrom <= session.getNum_servers();
        double p_w = (target_p_w * partitionsToReadFrom) / (1.0 - target_p_w + target_p_w * partitionsToReadFrom);
        int numPartitions = session.getNum_servers();
        double opTypeToss = Stress.randomizer.nextDouble();
        if (opTypeToss <= p_w) {
            write(clientLibrary, numPartitions);
        } else {
            read(clientLibrary, partitionsToReadFrom, numPartitions);
        }
    }

    public void read(ClientLibrary clientLibrary, int involvedServers, int totalServers) throws IOException {
        SlicePredicate nColumnsPredicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                false, 1));

        int pId = Stress.randomizer.nextInt(totalServers);

        List<ByteBuffer> keys = generateReadTxnKeys(totalServers, involvedServers, 1);
        ColumnParent parent = new ColumnParent("Standard1");

        int columnCount = 0;
        int bytesCount = 0;
        Map<ByteBuffer, List<ColumnOrSuperColumn>> results;


        long startNano = System.nanoTime();

        boolean success = false;
        String exceptionMessage = null;


        for (int t = 0; t < session.getRetryTimes(); ++t) {
            if (success)
                break;
            try {
                columnCount = 0;
                results = clientLibrary.transactional_multiget_slice(keys, parent, nColumnsPredicate);
                success = (results.size() == keys.size());
                if (!success)
                    exceptionMessage = "Wrong number of keys: " + results.size() + " instead of " + involvedServers;

                for (List<ColumnOrSuperColumn> result : results.values()) {
                    columnCount += result.size();
                    assert result.size() == 1;
                    for (ColumnOrSuperColumn cosc : result) {
                        bytesCount += ColumnOrSuperColumnHelper.findLength(cosc);
                    }
                }

            } catch (Exception e) {
                exceptionMessage = getExceptionMessage(e);
            }
        }
        if (!success) {
            error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                    index,
                    session.getRetryTimes(),
                    keys,
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(keys.size());
        session.columnCount.getAndAdd(columnCount);
        session.bytes.getAndAdd(bytesCount);
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano / 1000000);
        session.latencies.add(latencyNano / 1000);
        session.readlatencies.add(latencyNano / 1000);

    }

    public void write(ClientLibrary clientLibrary, int totalServers) throws IOException {
        if (value == null)
            value = generateValue();

        Column column = new Column(columnName(0, session.timeUUIDComparator)).setValue(value).setTimestamp(FBUtilities.timestampMicros());
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        int srvID = Stress.randomizer.nextInt(totalServers);

        ByteBuffer key = getZipfGeneratedKey(srvID);
        record.put(key, getColumnMutationMap(column));

        long startNano = System.nanoTime();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++) {
            if (success)
                break;
            try {
                clientLibrary.batch_mutate(record);
                success = true;
            } catch (Exception e) {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }
        if (!success) {
            error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                    index,
                    session.getRetryTimes(),
                    key,
                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.columnCount.getAndIncrement();
        session.bytes.getAndAdd(session.getColumnSize());
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano / 1000000);
        session.latencies.add(latencyNano / 1000);
        session.writelatencies.add(latencyNano / 1000);


    }

    private Map<String, List<Mutation>> getColumnMutationMap(Column c) {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();


        ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
        mutations.add(new Mutation().setColumn_or_supercolumn(column));


        mutationMap.put("Standard1", mutations);

        return mutationMap;
    }

}

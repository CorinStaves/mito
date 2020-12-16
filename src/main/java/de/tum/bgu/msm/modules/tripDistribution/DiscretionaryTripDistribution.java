package de.tum.bgu.msm.modules.tripDistribution;

import com.google.common.collect.Iterables;
import de.tum.bgu.msm.data.DataSet;
import de.tum.bgu.msm.data.MitoHousehold;
import de.tum.bgu.msm.data.Purpose;
import de.tum.bgu.msm.modules.Module;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.AirportDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.HbsHbrHboDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.NhbwNhboDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.RrtDistribution;
import de.tum.bgu.msm.util.concurrent.ConcurrentExecutor;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.log4j.Logger;
import org.matsim.core.utils.collections.Tuple;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static de.tum.bgu.msm.data.Purpose.*;

/**
 * @author Nico
 */
public final class DiscretionaryTripDistribution extends Module {

    public final static AtomicInteger distributedTripsCounter = new AtomicInteger(0);
    public final static AtomicInteger failedTripsCounter = new AtomicInteger(0);

    public final static AtomicInteger completelyRandomNhbTrips = new AtomicInteger(0);

    private final static  EnumSet<Purpose> DISCRETIONARY_PURPOSES = EnumSet.of(HBS,HBR,HBO,RRT,NHBW,NHBO);

    private EnumMap<Purpose, IndexedDoubleMatrix2D> utilityMatrices = new EnumMap<>(Purpose.class);

    private final static Logger logger = Logger.getLogger(DiscretionaryTripDistribution.class);

    public DiscretionaryTripDistribution(DataSet dataSet) {
        super(dataSet);
    }

    @Override
    public void run() {
        logger.info("Building initial destination choice utility matrices...");
        buildMatrices();

        logger.info("Distributing trips for households...");
        distributeTrips();
    }

    private void buildMatrices() {
        List<Callable<Tuple<Purpose,IndexedDoubleMatrix2D>>> utilityCalcTasks = new ArrayList<>();
        for (Purpose purpose : DISCRETIONARY_PURPOSES) {
            if (!purpose.equals(Purpose.AIRPORT)){
                //Distribution of trips to the airport does not need a matrix of weights
                utilityCalcTasks.add(new DestinationUtilityByPurposeGenerator(purpose, dataSet));
            }
        }
        ConcurrentExecutor<Tuple<Purpose, IndexedDoubleMatrix2D>> executor = ConcurrentExecutor.fixedPoolService(DISCRETIONARY_PURPOSES.size());
        List<Tuple<Purpose,IndexedDoubleMatrix2D>> results = executor.submitTasksAndWaitForCompletion(utilityCalcTasks);
        for(Tuple<Purpose, IndexedDoubleMatrix2D> result: results) {
            utilityMatrices.put(result.getFirst(), result.getSecond());
        }
        utilityMatrices.putAll(dataSet.getMandatoryUtilityMatrices());
    }

    private void distributeTrips() {
        final int numberOfThreads = Runtime.getRuntime().availableProcessors();
        final Collection<MitoHousehold> households = dataSet.getMobileHouseholds().values();

        final int partitionSize = (int) ((double) households.size() / (numberOfThreads)) + 1;
        Iterable<List<MitoHousehold>> partitions = Iterables.partition(households, partitionSize);

        logger.info("Using " + numberOfThreads + " thread(s)" +
                " with partitions of size " + partitionSize);

        // Home Based Trips
        ConcurrentExecutor<Void> executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);

        List<Callable<Void>> homeBasedTasks = new ArrayList<>();
        for (final List<MitoHousehold> partition : partitions) {
            homeBasedTasks.add(HbsHbrHboDistribution.hbs(utilityMatrices.get(HBS), partition, dataSet.getZones(),
                        dataSet.getTravelTimes(), dataSet.getPeakHour()));
            homeBasedTasks.add(HbsHbrHboDistribution.hbr(utilityMatrices.get(HBR), partition, dataSet.getZones(),
                        dataSet.getTravelTimes(), dataSet.getPeakHour()));
            homeBasedTasks.add(HbsHbrHboDistribution.hbo(utilityMatrices.get(HBO), partition, dataSet.getZones(),
                        dataSet.getTravelTimes(), dataSet.getPeakHour()));
        }
        executor.submitTasksAndWaitForCompletion(homeBasedTasks);

        // Recreational Round Trips
        executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);
        List<Callable<Void>> rrtTasks = new ArrayList<>();

        for (final List<MitoHousehold> partition : partitions) {
            rrtTasks.add(RrtDistribution.rrt(utilityMatrices, partition, dataSet.getZones(),
                    dataSet.getTravelTimes(), dataSet.getPeakHour()));
        }
        executor.submitTasksAndWaitForCompletion(rrtTasks);

        // Non Home Based Trips
        executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);
        List<Callable<Void>> nonHomeBasedTasks = new ArrayList<>();

        for (final List<MitoHousehold> partition : partitions) {
            nonHomeBasedTasks.add(NhbwNhboDistribution.nhbw(utilityMatrices, partition, dataSet.getZones(),
                        dataSet.getTravelTimes(), dataSet.getPeakHour()));
            nonHomeBasedTasks.add(NhbwNhboDistribution.nhbo(utilityMatrices, partition, dataSet.getZones(),
                        dataSet.getTravelTimes(), dataSet.getPeakHour()));
        }
        if (DISCRETIONARY_PURPOSES.contains(Purpose.AIRPORT)) {
            nonHomeBasedTasks.add(AirportDistribution.airportDistribution(dataSet));
        }
        executor.submitTasksAndWaitForCompletion(nonHomeBasedTasks);

        logger.info("Distributed: " + distributedTripsCounter + ", failed: " + failedTripsCounter);
        if(completelyRandomNhbTrips.get() > 0) {
            logger.info("There have been " + completelyRandomNhbTrips + " NHBO or NHBW trips" +
                    "by persons who don't have a matching home based trip. Assumed a destination for a suitable home based"
                    + " trip as either origin or destination for the non-home-based trip.");
        }
    }
}

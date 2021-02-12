package de.tum.bgu.msm.modules.tripDistribution;

import com.google.common.collect.Iterables;
import de.tum.bgu.msm.data.*;
import de.tum.bgu.msm.data.travelDistances.TravelDistances;
import de.tum.bgu.msm.io.input.readers.CoefficientReader;
import de.tum.bgu.msm.io.output.WriteZoneLogsums;
import de.tum.bgu.msm.modules.Module;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.HbsHbrHboDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.NhbwNhboDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.RrtDistribution;
import de.tum.bgu.msm.util.concurrent.ConcurrentExecutor;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;
import org.matsim.core.utils.collections.Tuple;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        logger.info("Loading mode choice model coefficients and zone attractions");
        loadModeChoiceAndZoneAttractions();

        logger.info("Preparing travel distance matrix");
        prepareTravelDistanceMatrix();

//        logger.info("Building initial destination choice utility matrices...");
//        buildMatrices();

        logger.info("Calculating logsum matrices");
        calculateLogsumMatrices();

//        logger.info("Distributing trips for households...");
//        distributeTrips();
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

    private void calculateLogsumMatrices() {
        final int numberOfThreads = Runtime.getRuntime().availableProcessors();
        ConcurrentExecutor<Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D>> executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);
        List<Callable<Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D>>> logsumTasks = new ArrayList<>();

        Collection<MitoPerson> persons = dataSet.getModelledPersons().values();

        for (Purpose purpose : HB_DISCRETIONARY_PURPOSES) {
            for (ModeRestriction modeRestriction : ModeRestriction.values()) {
                Collection<MitoPerson> includedPersons = persons.stream()
                        .filter(person -> person.hasTripsForPurpose(purpose))
                        .filter(person -> person.getModeRestriction().equals(modeRestriction))
                        .collect(Collectors.toUnmodifiableList());

                logsumTasks.add(new zoneLogsumAverages(purpose, modeRestriction, includedPersons, zones, travelDistancesNMT,
                        allZoneAttractions.get(purpose), allCoefficients.get(purpose), allDistanceCoefficients.get(purpose),
                        allNests.get(purpose), 1.));
            }
        }
        List<Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D>> results = executor.submitTasksAndWaitForCompletion(logsumTasks);
//        WriteZoneLogsums.writeAllLogsumData(zones, results);
        for(Triple<Purpose, ModeRestriction, IndexedDoubleMatrix2D> result : results) {
            WriteZoneLogsums.writeLogsumData(zones, result);
        }
    }

    private void distributeTrips() {
        final int numberOfThreads = Runtime.getRuntime().availableProcessors();
        final Collection<MitoHousehold> households = dataSet.getModelledHouseholds().values();

        final int partitionSize = (int) ((double) households.size() / (numberOfThreads)) + 1;
        Iterable<List<MitoHousehold>> partitions = Iterables.partition(households, partitionSize);

        logger.info("Using " + numberOfThreads + " thread(s)" +
                " with partitions of size " + partitionSize);

        // Home Based Trips
        ConcurrentExecutor<Void> executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);

        List<Callable<Void>> homeBasedTasks = new ArrayList<>();
        for (final List<MitoHousehold> partition : partitions) {
            homeBasedTasks.add(HbsHbrHboDistribution.hbs(utilityMatrices.get(HBS), partition, dataSet.getZones(),
                        dataSet.getTravelDistancesAuto(), dataSet.getPeakHour()));
            homeBasedTasks.add(HbsHbrHboDistribution.hbr(utilityMatrices.get(HBR), partition, dataSet.getZones(),
                        dataSet.getTravelDistancesAuto(), dataSet.getPeakHour()));
            homeBasedTasks.add(HbsHbrHboDistribution.hbo(utilityMatrices.get(HBO), partition, dataSet.getZones(),
                        dataSet.getTravelDistancesAuto(), dataSet.getPeakHour()));
        }
        executor.submitTasksAndWaitForCompletion(homeBasedTasks);

        // Recreational Round Trips
        executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);
        List<Callable<Void>> rrtTasks = new ArrayList<>();

        for (final List<MitoHousehold> partition : partitions) {
            rrtTasks.add(RrtDistribution.rrt(utilityMatrices, partition, dataSet.getZones(),
                    dataSet.getTravelDistancesNMT()));
        }
        executor.submitTasksAndWaitForCompletion(rrtTasks);

        // Non Home Based Trips
        executor = ConcurrentExecutor.fixedPoolService(numberOfThreads);
        List<Callable<Void>> nonHomeBasedTasks = new ArrayList<>();

        for (final List<MitoHousehold> partition : partitions) {
            nonHomeBasedTasks.add(NhbwNhboDistribution.nhbw(utilityMatrices, partition, dataSet.getZones(),
                        dataSet.getTravelDistancesAuto(), dataSet.getPeakHour()));
            nonHomeBasedTasks.add(NhbwNhboDistribution.nhbo(utilityMatrices, partition, dataSet.getZones(),
                        dataSet.getTravelDistancesAuto(), dataSet.getPeakHour()));
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

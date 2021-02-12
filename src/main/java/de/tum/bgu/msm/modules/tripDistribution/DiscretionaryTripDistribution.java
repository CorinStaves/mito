package de.tum.bgu.msm.modules.tripDistribution;

import com.google.common.collect.Iterables;
import de.tum.bgu.msm.data.*;
import de.tum.bgu.msm.data.travelDistances.TravelDistances;
import de.tum.bgu.msm.io.input.readers.CoefficientReader;
import de.tum.bgu.msm.io.output.WriteZoneLogsums;
import de.tum.bgu.msm.modules.Module;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.HbsHbrHboDistribution;
import de.tum.bgu.msm.modules.tripDistribution.destinationChooser.zoneLogsumAverages;
import de.tum.bgu.msm.resources.Resources;
import de.tum.bgu.msm.util.LogitTools;
import de.tum.bgu.msm.util.concurrent.ConcurrentExecutor;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix1D;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;
import org.matsim.core.utils.collections.Tuple;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static de.tum.bgu.msm.data.Mode.*;
import static de.tum.bgu.msm.data.Purpose.*;

/**
 * @author Nico
 */
public final class DiscretionaryTripDistribution extends Module {

    private final static Logger logger = Logger.getLogger(DiscretionaryTripDistribution.class);

    public final static AtomicInteger distributedTripsCounter = new AtomicInteger(0);
    public final static AtomicInteger failedTripsCounter = new AtomicInteger(0);
    public final static AtomicInteger completelyRandomNhbTrips = new AtomicInteger(0);

    private final static Path modeChoiceCoefFolder = Resources.instance.getModeChoiceCoefficients();
    private final static EnumSet<Mode> MODES = EnumSet.of(autoDriver,autoPassenger,publicTransport,bicycle,walk);
    private final static  EnumSet<Purpose> DISCRETIONARY_PURPOSES = EnumSet.of(HBS,HBR,HBO,RRT,NHBW,NHBO);
    private final static  EnumSet<Purpose> HB_DISCRETIONARY_PURPOSES = EnumSet.of(HBS,HBR,HBO);

    private final LogitTools logitTools = new LogitTools(Mode.class);

    private final Map<Integer, MitoZone> zones;
    private final IndexedDoubleMatrix2D travelDistancesNMT;

    private final EnumMap<Purpose, EnumMap<Mode, Map<String, Double>>> allCoefficients = new EnumMap<>(Purpose.class);
    private final EnumMap<Purpose, EnumMap<Mode, Double>> allDistanceCoefficients = new EnumMap<>(Purpose.class);
    private final EnumMap<Purpose, List<Tuple<EnumSet<Mode>, Double>>> allNests = new EnumMap<>(Purpose.class);
    private final EnumMap<Purpose, IndexedDoubleMatrix1D> allZoneAttractions = new EnumMap<>(Purpose.class);

    private EnumMap<Purpose, IndexedDoubleMatrix2D> utilityMatrices = new EnumMap<>(Purpose.class);


    public DiscretionaryTripDistribution(DataSet dataSet) {
        super(dataSet);
        this.zones = dataSet.getZones();
        this.travelDistancesNMT = new IndexedDoubleMatrix2D(zones.values(),zones.values());
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

    private void loadModeChoiceAndZoneAttractions() {
        for (Purpose purpose : HB_DISCRETIONARY_PURPOSES) {
            logger.info("Processing purpose: " + purpose);

            // Read model coefficients
            EnumMap<Mode, Map<String, Double>> coefficientsByMode = new EnumMap<>(Mode.class);
            EnumMap<Mode, Double> distanceCoefficientsByMode = new EnumMap<>(Mode.class);
            for (Mode mode : MODES) {
                Map<String, Double> coefficients = new CoefficientReader(dataSet, mode,
                        modeChoiceCoefFolder.resolve(purpose.toString() + ".csv")).readCoefficients();
                coefficientsByMode.put(mode, coefficients);
                distanceCoefficientsByMode.put(mode, coefficients.get("t.distance_T"));
            }
            allNests.put(purpose, logitTools.identifyNests(coefficientsByMode));
            allCoefficients.put(purpose, coefficientsByMode);
            allDistanceCoefficients.put(purpose, distanceCoefficientsByMode);

            // Create zone attractions vector
            IndexedDoubleMatrix1D zoneAttractions = new IndexedDoubleMatrix1D(zones.values());
            for (MitoZone zone : zones.values()) {
                zoneAttractions.setIndexed(zone.getId(), zone.getTripAttraction(purpose));
            }
            allZoneAttractions.put(purpose, zoneAttractions);
        }
    }

    private void prepareTravelDistanceMatrix() {
        TravelDistances travelDistances = dataSet.getTravelDistancesNMT();
        for (MitoZone origin : zones.values()) {
            for (MitoZone destination : zones.values()) {
                travelDistancesNMT.setIndexed(origin.getId(),destination.getId(),
                        travelDistances.getTravelDistance(origin.getId(), destination.getId()));
            }
        }
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
            homeBasedTasks.add(HbsHbrHboDistribution.hbs(partition, dataSet.getZones(), travelDistancesNMT,
                    allZoneAttractions.get(HBS), allCoefficients.get(HBS), allDistanceCoefficients.get(HBS), allNests.get(HBS)));
            homeBasedTasks.add(HbsHbrHboDistribution.hbr(partition, dataSet.getZones(), travelDistancesNMT,
                    allZoneAttractions.get(HBR), allCoefficients.get(HBR), allDistanceCoefficients.get(HBR), allNests.get(HBR)));
            homeBasedTasks.add(HbsHbrHboDistribution.hbo(partition, dataSet.getZones(), travelDistancesNMT,
                    allZoneAttractions.get(HBO), allCoefficients.get(HBO), allDistanceCoefficients.get(HBO), allNests.get(HBO)));
        }
        executor.submitTasksAndWaitForCompletion(homeBasedTasks);

/*
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
        }*/
    }
}

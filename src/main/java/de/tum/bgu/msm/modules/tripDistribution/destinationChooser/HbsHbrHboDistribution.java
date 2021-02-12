package de.tum.bgu.msm.modules.tripDistribution.destinationChooser;

import com.google.common.math.LongMath;
import de.tum.bgu.msm.data.*;
import de.tum.bgu.msm.data.travelDistances.TravelDistances;
import de.tum.bgu.msm.data.travelTimes.TravelTimes;
import de.tum.bgu.msm.modules.tripDistribution.DiscretionaryTripDistribution;
import de.tum.bgu.msm.modules.tripDistribution.calibration.HbsHbrHboDistributionCalibration;
import de.tum.bgu.msm.util.MitoUtil;
import de.tum.bgu.msm.util.concurrent.RandomizableConcurrentFunction;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix1D;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author Nico
 */
public class HbsHbrHboDistribution extends RandomizableConcurrentFunction<Void> {

    private final static double VARIANCE_DOUBLED = 30 * 2;
    private final static double SQRT_INV = 1.0 / Math.sqrt(Math.PI * VARIANCE_DOUBLED);

    private final static Logger logger = Logger.getLogger(HbsHbrHboDistribution.class);

    private final Purpose purpose;
    private final double peakHour;
    private final double speedInv;

    private final IndexedDoubleMatrix2D baseProbabilities;
    private final TravelDistances travelDistances;

    private final Collection<MitoHousehold> householdPartition;
    private final Map<Integer, MitoZone> zonesCopy;

    private final double[] destinationProbabilities;
    private final double[] zoneAttractions;

    private double idealBudgetSum = 0;
    private double actualBudgetSum = 0;
    private double personBudgetPerTrip;
    private double mean;

    private HbsHbrHboDistribution(Purpose purpose, IndexedDoubleMatrix2D baseProbabilities,
                                  Collection<MitoHousehold> householdPartition, Map<Integer, MitoZone> zones,
                                  TravelDistances travelDistances, double peakHour, double speedInv) {
        super(MitoUtil.getRandomObject().nextLong());
        this.purpose = purpose;
        this.householdPartition = householdPartition;
        this.baseProbabilities = baseProbabilities;
        this.zonesCopy = new HashMap<>(zones);
        this.destinationProbabilities = new double[baseProbabilities.columns()];
        this.travelDistances = travelDistances;
        this.peakHour = peakHour;
        this.speedInv = speedInv;
    }

    public static HbsHbrHboDistribution hbs(IndexedDoubleMatrix2D baseProbabilities, Collection<MitoHousehold> householdPartition, Map<Integer, MitoZone> zones,
                                                       TravelDistances travelDistances, double peakHour) {
        return new HbsHbrHboDistribution(Purpose.HBS, baseProbabilities, householdPartition, zones, travelDistances, peakHour, 60/19.051);
    }

    public static HbsHbrHboDistribution hbo(IndexedDoubleMatrix2D baseProbabilities, Collection<MitoHousehold> householdPartition, Map<Integer, MitoZone> zones,
                                                       TravelDistances travelDistances, double peakHour) {
        return new HbsHbrHboDistribution(Purpose.HBO, baseProbabilities, householdPartition, zones, travelDistances, peakHour,60/24.713);
    }

    public static HbsHbrHboDistribution hbr(IndexedDoubleMatrix2D baseProbabilities, Collection<MitoHousehold> householdPartition, Map<Integer, MitoZone> zones,
                                                       TravelDistances travelDistances, double peakHour) {
        return new HbsHbrHboDistribution(Purpose.HBR, baseProbabilities, householdPartition, zones, travelDistances, peakHour,60/24.805);
    }

    @Override
    public Void call() {
        long counter = 0;
        for (MitoHousehold household : householdPartition) {
            if (LongMath.isPowerOfTwo(counter)) {
                logger.info(counter + " households done for Purpose " + purpose
                        + "\nIdeal budget sum: " + idealBudgetSum + " | actual budget sum: " + actualBudgetSum);
            }
            for(MitoPerson person : household.getPersons().values()) {
                if(person.getTripsForPurpose(purpose).size() > 0) {
                    if(person.getTravelTimeBudgetForPurpose(purpose) > 0) {
                        updateBudgets(person);
                        for (MitoTrip trip : person.getTripsForPurpose(purpose)) {
                            trip.setTripOrigin(household);
                            MitoZone destination = findDestination(household.getHomeZone().getId());
                            trip.setTripDestination(destination);
                            if (destination == null) {
                                logger.debug("No destination found for trip" + trip);
                                DiscretionaryTripDistribution.failedTripsCounter.incrementAndGet();
                                continue;
                            }
                            postProcessTrip(trip);
                            DiscretionaryTripDistribution.distributedTripsCounter.incrementAndGet();
                        }
                    } else {
                        logger.warn("Person " + person.getId() + " has " + purpose + " trips but no TTB!");
                        DiscretionaryTripDistribution.failedTripsCounter.incrementAndGet();
                    }
                }
            }
            counter++;
        }
        return null;
    }

    private void postProcessTrip(MitoTrip trip) {
        actualBudgetSum += travelDistances.getTravelDistance(trip.getTripOrigin().getZoneId(),
                trip.getTripDestination().getZoneId()) * speedInv * 2;
        idealBudgetSum += personBudgetPerTrip;
    }

    private MitoZone findDestination(int origin) {
        final IndexedDoubleMatrix1D row = baseProbabilities.viewRow(origin);
        double[] baseProbs = row.toNonIndexedArray();
        IntStream.range(0, destinationProbabilities.length).parallel().forEach(i -> {
            //multiply travel time by 2 as home based trips' budget account for the return trip as well
            double diff = travelDistances.getTravelDistance(zonesCopy.get(origin).getId(), zonesCopy.get(row.getIdForInternalIndex(i)).getId()) * speedInv * 2 - mean;
            double factor = SQRT_INV * FastMath.exp(-(diff * diff) / VARIANCE_DOUBLED);
            destinationProbabilities[i] = baseProbs[i] * factor;
        });

        final int destinationInternalIndex = MitoUtil.select(destinationProbabilities, random);
        return zonesCopy.get(baseProbabilities.getIdForInternalColumnIndex(destinationInternalIndex));
    }

    private void updateBudgets(MitoPerson person) {
        double ratio;
        if (idealBudgetSum == actualBudgetSum) {
            ratio = 1;
        } else {
            ratio = idealBudgetSum / actualBudgetSum;
        }
        personBudgetPerTrip = person.getTravelTimeBudgetForPurpose(purpose) / person.getTripsForPurpose(purpose).size();
        mean = personBudgetPerTrip * ratio;
    }
}


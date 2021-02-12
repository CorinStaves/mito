package de.tum.bgu.msm.modules.tripDistribution;

import com.google.common.math.IntMath;
import de.tum.bgu.msm.data.*;
import de.tum.bgu.msm.data.travelDistances.TravelDistances;
import de.tum.bgu.msm.util.LogitTools;
import de.tum.bgu.msm.util.MitoUtil;
import de.tum.bgu.msm.util.concurrent.RandomizableConcurrentFunction;

import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix1D;
import de.tum.bgu.msm.util.matrices.IndexedDoubleMatrix2D;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;
import org.matsim.core.utils.collections.Tuple;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import static de.tum.bgu.msm.data.Mode.*;
import static de.tum.bgu.msm.data.Purpose.*;

public class logsumCalculations implements Callable<Map<MitoPerson, float[]>> {

    private final static Logger logger = Logger.getLogger(logsumCalculations.class);
    private final static double SCALE_PARAMETER = 0.1;

    private final Collection<MitoPerson> personPartition;
    private final Map<Integer, MitoZone> zones;
    private final IndexedDoubleMatrix2D travelDistancesNMT;
    private final Purpose purpose;
    private final EnumMap<Mode, Map<String, Double>> coefficients;
    private final EnumMap<Mode, Double> distanceCoefficientsMap;
    private final List<Tuple<EnumSet<Mode>, Double>> nests;
    private final Map<MitoPerson, float[]> logsumMap = new HashMap<>();

    private double[] distances;

    protected logsumCalculations(Purpose purpose,
                                 Collection<MitoPerson> personPartition,
                                 Map<Integer, MitoZone> zones,
                                 IndexedDoubleMatrix2D travelDistancesNMT,
                                 EnumMap<Mode, Map<String, Double>> coefficients,
                                 EnumMap<Mode, Double> distanceCoefficients,
                                 List<Tuple<EnumSet<Mode>, Double>> nests) {
        this.personPartition = personPartition;
        this.purpose = purpose;
        this.zones = zones;
        this.travelDistancesNMT = travelDistancesNMT;
        this.coefficients = coefficients;
        this.distanceCoefficientsMap = distanceCoefficients;
        this.nests = nests;
    }

    @Override
    public Map<MitoPerson, float[]> call() {
        int counter = 0;
        for (MitoPerson person : personPartition) {
            if(IntMath.isPowerOfTwo(counter)) {
                logger.info(counter + " persons done for purpose " + purpose);
            }
            if(person.getTripsForPurpose(purpose).size() > 0) {
//                EnumSet<Mode> availableChoices = determineAvailability(person);
//                int choices = availableChoices.size();
//                int[] modeMap = new int[choices];
//                double[] personUtilities = new double[choices];
//                double[] distanceCoefficients = new double[choices];
//
//                int m = 0;
//                for(Mode mode : availableChoices) {
//                    modeMap[m] = mode.getId();
//                    personUtilities[m] = getPersonPredictor(person, coefficients.get(mode));
//                    distanceCoefficients[m] = distanceCoefficientsMap.get(mode);
//                    m++;
//                }

                EnumMap<Mode, Double> personUtilities = getPersonUtilities(person);
                List<Tuple<EnumSet<Mode>, Double>> personNests = new ArrayList<>();
                for(Tuple<EnumSet<Mode>,Double> nest : nests) {
                    EnumSet<Mode> personNest = EnumSet.copyOf(nest.getFirst());
                    personNest.retainAll(personUtilities.keySet());
                    personNests.add(new Tuple<>(personNest, nest.getSecond()));
                }

                distances = travelDistancesNMT.viewRow(person.getHousehold().getHomeZone().getId()).toNonIndexedArray();
                float[] logsums = new float[distances.length];
                IntStream.range(0, distances.length).parallel().forEach(i -> {
                    double logDistance = FastMath.log(distances[i]);
                    double expSumRoot = 0.;
                    for(Tuple<EnumSet<Mode>,Double> nest : personNests) {
                        double expNestSum = 0;
                        double nestScaleParameter = SCALE_PARAMETER / nest.getSecond();
                        for(Mode mode : nest.getFirst()) {
                            double expOptionUtil = FastMath.exp(nestScaleParameter *
                                    (personUtilities.get(mode) + logDistance * distanceCoefficientsMap.get(mode)));
                            expNestSum += expOptionUtil;
                        }
                        double expNestUtil = FastMath.exp(SCALE_PARAMETER * FastMath.log(expNestSum) / nestScaleParameter);
                        expSumRoot += expNestUtil;
                    }
                    logsums[i] = (float) (FastMath.log(expSumRoot) / SCALE_PARAMETER);
                });
                logsumMap.put(person, logsums);
            } else {
                logsumMap.put(person, null);
            }
            counter++;
        }
        return logsumMap;
    }

    private EnumSet<Mode> determineAvailability(MitoPerson person) {
        EnumSet<Mode> availability = person.getModeRestriction().getRestrictedModeSet();

        if(purpose.equals(RRT)) {
            availability.removeAll(EnumSet.of(autoDriver, autoPassenger, publicTransport));
        }

        return availability;
    }

    private EnumMap<Mode, Double> getPersonUtilities(MitoPerson person) {

        EnumMap<Mode, Double> utilities = new EnumMap(Mode.class);

        EnumSet<Mode> availableChoices = determineAvailability(person);
        for(Mode mode : availableChoices) {
            utilities.put(mode, getPersonPredictor(person, coefficients.get(mode)));
        }

        return (utilities);
    }

    private EnumMap<Mode, Double> getTripUtilities(EnumMap<Mode, Double> personUtilities, double distance) {

        EnumMap<Mode, Double> utilities = new EnumMap(Mode.class);

        for (Mode mode : personUtilities.keySet()) {
            utilities.put(mode, personUtilities.get(mode) + (Math.log(distance) * distanceCoefficientsMap.get(mode)));
        }

        return utilities;
    }

    private double getPersonPredictor(MitoPerson pp, Map<String, Double> coefficients) {
        double predictor = 0.;
        MitoHousehold hh = pp.getHousehold();

        // Intercept
        predictor += coefficients.get("INTERCEPT");

        // Household size
        int householdSize = hh.getHhSize();
        if (householdSize == 1) {
            predictor += coefficients.getOrDefault("hh.size_1",0.);
        } else if (householdSize == 2) {
            predictor += coefficients.getOrDefault("hh.size_2",0.);
        } else if (householdSize == 3) {
            predictor += coefficients.getOrDefault("hh.size_3",0.);
        } else if (householdSize == 4) {
            predictor += coefficients.getOrDefault("hh.size_4",0.);
        } else if (householdSize >= 5) {;
            predictor += coefficients.getOrDefault("hh.size_5",0.);
        }

        // Number of children in household
        int householdChildren = DataSet.getChildrenForHousehold(hh);
        if(householdChildren == 1) {
            predictor += coefficients.getOrDefault("hh.children_1",0.);
        } else if (householdChildren == 2) {
            predictor += coefficients.getOrDefault("hh.children_2",0.);
        } else if (householdChildren >= 3) {
            predictor += coefficients.getOrDefault("hh.children_3",0.);
        }

        // Economic status
        int householdEconomicStatus = hh.getEconomicStatus();
        if (householdEconomicStatus == 2) {
            predictor += coefficients.getOrDefault("hh.econStatus_2",0.);
        } else if (householdEconomicStatus == 3) {
            predictor += coefficients.getOrDefault("hh.econStatus_3",0.);
        } else if (householdEconomicStatus == 4) {
            predictor += coefficients.getOrDefault("hh.econStatus_4",0.);
        }

        // Household in urban region
        if(!(hh.getHomeZone().getAreaTypeR().equals(AreaTypes.RType.RURAL))) {
            predictor += coefficients.getOrDefault("hh.urban",0.);
        }

        // Autos
        int householdAutos = hh.getAutos();
        if (householdAutos == 1) {
            predictor += coefficients.getOrDefault("hh.cars_1",0.);
        } else if (householdAutos == 2) {
            predictor += coefficients.getOrDefault("hh.cars_2",0.);
        } else if (householdAutos >= 3) {
            predictor += coefficients.getOrDefault("hh.cars_3",0.);
        }

        // Autos per adult
        int householdAdults = hh.getHhSize() - householdChildren;
        double autosPerAdult = Math.min((double) householdAutos / householdAdults , 1.);
        predictor += autosPerAdult * coefficients.getOrDefault("hh.autosPerAdult",0.);

        // Is home close to PT?
        if(coefficients.containsKey("hh.homePT")) {
            int homeZoneId = hh.getHomeZone().getId();
            double homeDistanceToPT = zones.get(homeZoneId).getDistanceToNearestRailStop();
            double homeWalkToPT = homeDistanceToPT * (60 / 4.8);
            if(homeWalkToPT <= 20) {
                predictor += coefficients.get("hh.homePT");
            }
        }

        // Is the place of work or study close to PT?
        if(coefficients.containsKey("p.workPT_12")) {
            if(pp.getOccupation() != null) {
                int occupationZoneId = pp.getOccupation().getZoneId();
                double occupationDistanceToPT = zones.get(occupationZoneId).getDistanceToNearestRailStop();
                double occupationWalkToPT = occupationDistanceToPT * (60 / 4.8);
                if(occupationWalkToPT <= 20) {
                    predictor += coefficients.get("p.workPT_12");
                }
            }
        }

        // Age
        int age = pp.getAge();
        if (age <= 18) {
            predictor += coefficients.getOrDefault("p.age_gr_1",0.);
        }
        else if (age <= 29) {
            predictor += coefficients.getOrDefault("p.age_gr_2",0.);
        }
        else if (age <= 49) {
            predictor += 0.;
        }
        else if (age <= 59) {
            predictor += coefficients.getOrDefault("p.age_gr_4",0.);
        }
        else if (age <= 69) {
            predictor += coefficients.getOrDefault("p.age_gr_5",0.);
        }
        else {
            predictor += coefficients.getOrDefault("p.age_gr_6",0.);
        }

        // Female
        if(pp.getMitoGender().equals(MitoGender.FEMALE)) {
            predictor += coefficients.getOrDefault("p.female",0.);
        }

        // Has drivers Licence
        if(pp.hasDriversLicense()) {
            predictor += coefficients.getOrDefault("p.driversLicence",0.);
        }

        // Has bicycle
        if(pp.hasBicycle()) {
            predictor += coefficients.getOrDefault("p.ownBicycle",0.);
        }

        // Number of mandatory trips
        int trips_HBW = pp.getTripsForPurpose(Purpose.HBW).size();

        if (trips_HBW == 0) {
            predictor += coefficients.getOrDefault("p.trips_HBW_0",0.);
        } else if (trips_HBW < 5) {
            predictor += coefficients.getOrDefault("p.trips_HBW_1234",0.);
            predictor += coefficients.getOrDefault("p.isMobile_HBW",0.);
        } else {
            predictor += coefficients.getOrDefault("p.trips_HBW_5",0.);
            predictor += coefficients.getOrDefault("p.isMobile_HBW",0.);
        }

        int trips_HBE = pp.getTripsForPurpose(Purpose.HBE).size();

        if (trips_HBE == 0) {
            predictor += 0;
        } else if (trips_HBE < 5) {
            predictor += coefficients.getOrDefault("p.trips_HBE_1324",0.);
            predictor += coefficients.getOrDefault("p.isMobile_HBE",0.);
        } else {
            predictor += coefficients.getOrDefault("p.trips_HBE_5",0.);
            predictor += coefficients.getOrDefault("p.isMobile_HBE",0.);
        }

        // Usual commute mode
        Mode dominantCommuteMode = pp.getDominantCommuteMode();
        if(dominantCommuteMode != null) {
            if (dominantCommuteMode.equals(Mode.autoDriver)) {
                predictor += coefficients.getOrDefault("p.usualCommuteMode_carD",0.);
            } else if (dominantCommuteMode.equals(Mode.autoPassenger)) {
                predictor += coefficients.getOrDefault("p.usualCommuteMode_carP",0.);
            } else if (dominantCommuteMode.equals(Mode.publicTransport)) {
                predictor += coefficients.getOrDefault("p.usualCommuteMode_PT",0.);
            } else if (dominantCommuteMode.equals(Mode.bicycle)) {
                predictor += coefficients.getOrDefault("p.usualCommuteMode_cycle",0.);
            } else if (dominantCommuteMode.equals(Mode.walk)) {
                predictor += coefficients.getOrDefault("p.usualCommuteMode_walk",0.);
            }
        }

        return predictor;
    }


}

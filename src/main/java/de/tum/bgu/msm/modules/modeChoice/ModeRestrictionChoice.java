package de.tum.bgu.msm.modules.modeChoice;

import de.tum.bgu.msm.data.*;
import de.tum.bgu.msm.io.input.readers.CoefficientReader;
import de.tum.bgu.msm.modules.Module;
import de.tum.bgu.msm.resources.Resources;
import de.tum.bgu.msm.util.LogitTools;
import de.tum.bgu.msm.util.MitoUtil;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.util.*;

import static de.tum.bgu.msm.data.ModeRestriction.*;


public class ModeRestrictionChoice extends Module {

    private final static Logger logger = Logger.getLogger(ModeChoice.class);
    private final static Path modeRestrictionCoefPath = Resources.instance.getModeRestrictionCoefficients();

    private final EnumMap<ModeRestriction, Map<String, Double>> coefficients = new EnumMap<>(ModeRestriction.class);
    private final LogitTools logitTools = new LogitTools(ModeRestriction.class);
    private int nonMobilePersons;


    public ModeRestrictionChoice(DataSet dataSet) {
        super(dataSet);
        for(ModeRestriction modeRestriction : ModeRestriction.values()) {
            coefficients.put(modeRestriction, new CoefficientReader(dataSet, modeRestriction, modeRestrictionCoefPath).readCoefficients());
        }
    }

    @Override
    public void run() {
        logger.info(" Calculating mode restriction.");
        modeRestriction();
    }

    private void modeRestriction() {

        for (MitoPerson person : dataSet.getModelledPersons().values()) {
            if(person.getTrips().size() == 0) {
                nonMobilePersons++;
            } else {
                chooseModeRestriction(person, logitTools.getProbabilitiesMNL(getUtilities(person)));
            }
        }
        logger.info(nonMobilePersons + " non-mobile persons skipped");
    }

    private EnumMap<ModeRestriction, Double> getUtilities(MitoPerson person) {

        EnumMap<ModeRestriction, Double> utilities = new EnumMap(ModeRestriction.class);

        EnumSet<ModeRestriction> availableChoices = determineAvailability(person);
        for(ModeRestriction modeRestriction : availableChoices) {
            utilities.put(modeRestriction, getPredictor(person, coefficients.get(modeRestriction)));
        }

        return (utilities);
    }

    private EnumSet<ModeRestriction> determineAvailability(MitoPerson person) {

        EnumSet<ModeRestriction> availableChoices = EnumSet.noneOf(ModeRestriction.class);
        Mode dominantCommuteMode = person.getDominantCommuteMode();

        boolean commuter = dominantCommuteMode != null;

        EnumSet<ModeRestriction> possibleOptions;
        if(person.hasTripsForPurpose(Purpose.RRT)) {
            possibleOptions = EnumSet.of(autoPtWalk, autoPtWalkCycle, ptWalk, ptWalkCycle);
        } else {
            possibleOptions = EnumSet.allOf(ModeRestriction.class);
        }

        if(commuter) {
            for (ModeRestriction modeRestriction : possibleOptions) {
                if (modeRestriction.getRestrictedModeSet().contains(dominantCommuteMode)) {
                    availableChoices.add(modeRestriction);
                }
            }
        } else {
            availableChoices.addAll(possibleOptions);
        }
        return availableChoices;
    }

    private double getPredictor(MitoPerson pp, Map<String, Double> coefficients) {
        double predictor = 0.;
        MitoHousehold hh = pp.getHousehold();

        // Intercept
        predictor += coefficients.get("INTERCEPT");

        // Number of children in household
        int householdChildren = DataSet.getChildrenForHousehold(hh);
        if(householdChildren == 2) {
            predictor += coefficients.get("hh.children_2");
        } else if (householdChildren >= 3) {
            predictor += coefficients.get("hh.children_3");
        }

        // Economic status
        int householdEconomicStatus = hh.getEconomicStatus();
        if (householdEconomicStatus == 2) {
            predictor += coefficients.get("hh.econStatus_2");
        } else if (householdEconomicStatus == 3) {
            predictor += coefficients.get("hh.econStatus_3");
        } else if (householdEconomicStatus == 4) {
            predictor += coefficients.get("hh.econStatus_4");
        }

        // Household in urban region
        if(!(hh.getHomeZone().getAreaTypeR().equals(AreaTypes.RType.RURAL))) {
            predictor += coefficients.get("hh.urban");
        }

        // Autos
        int householdAutos = hh.getAutos();
        if (householdAutos == 1) {
            predictor += coefficients.get("hh.cars_1");
        } else if (householdAutos == 2) {
            predictor += coefficients.get("hh.cars_2");
        } else if (householdAutos >= 3) {
            predictor += coefficients.get("hh.cars_3");
        }

        // Autos per adult
        int householdAdults = hh.getHhSize() - householdChildren;
        double autosPerAdult = Math.min((double) householdAutos / householdAdults , 1.);
        predictor += autosPerAdult * coefficients.get("hh.autosPerAdult");

        // Is home close to PT?
        int homeZoneId = hh.getHomeZone().getId();
        double homeDistanceToPT = dataSet.getZones().get(homeZoneId).getDistanceToNearestRailStop();
        double homeWalkToPT = homeDistanceToPT * (60 / 4.8);
        if(homeWalkToPT <= 20) {
            predictor += coefficients.get("hh.homePT");
        }

        // Is work close to PT?
        if(pp.getOccupation() != null) {
            int occupationZoneId = pp.getOccupation().getZoneId();
            double occupationDistanceToPT = dataSet.getZones().get(occupationZoneId).getDistanceToNearestRailStop();
            double occupationWalkToPT = occupationDistanceToPT * (60 / 4.8);
            if(occupationWalkToPT <= 20) {
                predictor += coefficients.get("p.workPT_12");
            }
        }

        // Age
        int age = pp.getAge();
        if (age <= 18) {
            predictor += coefficients.get("p.age_gr_1");
        }
        else if (age <= 29) {
            predictor += coefficients.get("p.age_gr_2");
        }
        else if (age <= 49) {
            predictor += 0.;
        }
        else if (age <= 59) {
            predictor += coefficients.get("p.age_gr_4");
        }
        else if (age <= 69) {
            predictor += coefficients.get("p.age_gr_5");
        }
        else {
            predictor += coefficients.get("p.age_gr_6");
        }

        // Female
        if(pp.getMitoGender().equals(MitoGender.FEMALE)) {
            predictor += coefficients.get("p.female");
        }

        // Has drivers Licence
        if(pp.hasDriversLicense()) {
            predictor += coefficients.get("p.driversLicense");
        }

        // Has bicycle
        if(pp.hasBicycle()) {
            predictor += coefficients.get("p.ownBicycle");
        }

        // Number of commute trips
        List<MitoTrip> workTrips = pp.getTripsForPurpose(Purpose.HBW);
        int workTripCount = workTrips.size();
        if (workTripCount > 0 && workTripCount < 5) {
            predictor += coefficients.get("p.workTrips_1234");
        } else if (workTripCount >= 5) {
            predictor += coefficients.get("p.workTrips_5");
        }

        List<MitoTrip> educationTrips = pp.getTripsForPurpose(Purpose.HBE);
        int educationTripCount = educationTrips.size();
        if (educationTripCount > 0 && educationTripCount < 5) {
            predictor += coefficients.get("p.eduTrips_1234");
        } else if (educationTripCount >= 5) {
            predictor += coefficients.get("p.eduTrips_5");
        }

        // Number of discretionary trips
        predictor += Math.sqrt(pp.getTripsForPurpose(Purpose.HBS).size()) * coefficients.get("p.trips_HBS_T");
        predictor += Math.sqrt(pp.getTripsForPurpose(Purpose.HBR).size()) * coefficients.get("p.trips_HBR_T");
        predictor += Math.sqrt(pp.getTripsForPurpose(Purpose.HBO).size()) * coefficients.get("p.trips_HBO_T");
        predictor += Math.sqrt(pp.getTripsForPurpose(Purpose.NHBW).size()) * coefficients.get("p.trips_NHBW_T");
        predictor += Math.sqrt(pp.getTripsForPurpose(Purpose.NHBO).size()) * coefficients.get("p.trips_NHBO_T");
        if(pp.hasTripsForPurpose(Purpose.RRT)) predictor += coefficients.get("p.isMobile_RRT");

        // Mean commute trip distance
        List<MitoTrip> commuteTrips = new ArrayList(workTrips);
        commuteTrips.addAll(educationTrips);
        int commuteTripCount = commuteTrips.size();

        if (commuteTripCount > 0) {
            ArrayList<Double> commuteTripDistances = new ArrayList<>();
            double sum = 0;
            for (MitoTrip trip : commuteTrips) {
                double commuteDistance = dataSet.getTravelDistancesNMT().
                        getTravelDistance(trip.getTripOrigin().getZoneId(),trip.getTripDestination().getZoneId());
                commuteTripDistances.add(commuteDistance);
                sum += commuteDistance;
            }
            double mean = sum / commuteTripCount;
            double sqrtMean = Math.sqrt(mean);

            predictor += sqrtMean * coefficients.get("p.m_km_mode_T");
        }

        // Usual commute mode
        Mode dominantCommuteMode = pp.getDominantCommuteMode();
        if(dominantCommuteMode != null) {
            if (dominantCommuteMode.equals(Mode.autoDriver)) {
                predictor += 0.;
            } else if (dominantCommuteMode.equals(Mode.autoPassenger)) {
                predictor += coefficients.get("p.usualCommuteMode_carP");
            } else if (dominantCommuteMode.equals(Mode.publicTransport)) {
                predictor += coefficients.get("p.usualCommuteMode_PT");
            } else if (dominantCommuteMode.equals(Mode.bicycle)) {
                predictor += coefficients.get("p.usualCommuteMode_cycle");
            } else if (dominantCommuteMode.equals(Mode.walk)) {
                predictor += coefficients.get("p.usualCommuteMode_walk");
            }
        }

        return predictor;
    }

    private void chooseModeRestriction(MitoPerson person, EnumMap<ModeRestriction, Double> probabilities) {
        double sum = MitoUtil.getSum(probabilities.values());
        if (Math.abs(sum - 1) > 0.1) {
            logger.warn("Mode probabilities don't add to 1 for person " + person.getId());
        }
        if (sum > 0) {
            final ModeRestriction select = MitoUtil.select(probabilities, MitoUtil.getRandomObject());
            person.setModeRestriction(select);
        } else {
            logger.error("Zero/negative probabilities for person " + person.getId());
            person.setModeRestriction(null);
        }
    }
}
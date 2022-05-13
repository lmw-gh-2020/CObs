using System;
using System.Collections.Generic;
using System.Linq;

/*
*
* Author:  lmw.gh.2020@gmail.com, all rights reserved, October 2020
* License: Apache License, Version 2.0
*
* https://opensource.org/licenses/Apache-2.0 
*
*/

namespace CObs
{
    public enum SourcedOn
    {
         Unknown
        ,MortalityOnly
        ,MortalityAndAdmissions
        ,AdmissionsOnly
        ,TestResults
        ,ProjectedCases
    }

    public class DayWithAggregates
    {
        /*
            (+ve/-ve) infinite growth rate, (i.e. at zero incidence) is stored as zero
            (+ve/-ve) infinite doubling time, (i.e. at R-eff = 1) is stored as zero
        */

        public Scenario   Parent                    { get; private set; }
        public DayRaw     Raw                       { get; private set; }
        public DayRolling Rolling                   { get; private set; }

        public SourcedOn  SourcedOn                 { get; set; }
        public int        AdmissionsWithChurn       { get; set; }
        public int        AdmissionsWithChurnSmooth { get; set; }
        public int        ActualDNC                 { get; set; }
        public int        CasesPerAdmission         { get; set; }
        public double     Rolling9DayDeltaCDeltaT   { get; set; }

        public double     GrowthRate                { get; set; }
        public double     DoublingTime              { get; set; }
        public double     REff                      { get; set; }

        public DayWithAggregates(
             Scenario   pParent
            ,DayRolling pRolling
        ) {
            Parent    = pParent;
            Raw       = pRolling.Raw;
            Rolling   = pRolling;

            SourcedOn = SourcedOn.Unknown;
        }
    }

    public class DayADNCRunUp
    {
        public int       TimelineIndex { get; set; }
        public DateTime  Date          { get; set; }
        public SourcedOn SourcedOn     { get; set; }
        public int       ActualDNC     { get; set; }

        public DayADNCRunUp(
             int      pIndex
            ,DateTime pDate
        ) {
            TimelineIndex = pIndex;
            Date          = pDate;

            SourcedOn     = SourcedOn.MortalityOnly;
        }
    }

    public class Scenario
    {
        /* daily results */
        public  List<DayWithAggregates> DaysWithAggregates            { get; private set; }
        public  List<DayADNCRunUp>      RunUpDays                     { get; private set; }

        /* global growth aggregates */
        public  double                  CurrentGrowthRate             { get; private set; }
        public  decimal                 CurrentREff                   { get; private set; }

        /* global linear aggregates */
        public  decimal                 ProjectedTotalSeroprev        { get; private set; }
        public  int                     ProjectedTotalMortality       { get; private set; }

        /* parameters and source data */
        private double                  IFR                           { get; set; }
        private int                     MedianTimeFromExposureToTest  { get; set; }
        private int                     MedianTimeToAdmission         { get; set; }
        private int                     MedianHospitalizationDuration { get; set; }
        private int                     MedianTimeToMortality         { get; set; }
        private double                  MedianSerialInterval          { get; set; }
        private double                  TransmissibilityWindowToSI    { get; set; }

        private List<DayRaw>            DaysRaw                       { get; set; }
        private List<DayRolling>        DaysRolling                   { get; set; }

        public Scenario(
             BaseDays           pBaseDays
            ,ScenarioParameters pScenarioParameters
        ) {
            DaysRaw                       = pBaseDays.DaysRaw;
            DaysRolling                   = pBaseDays.DaysRolling;

            IFR                           = pScenarioParameters.IFR;
            MedianTimeFromExposureToTest  = pScenarioParameters.MedianTimeFromExposureToTest;
            MedianTimeToAdmission         = pScenarioParameters.MedianTimeToAdmission;
            MedianHospitalizationDuration = pScenarioParameters.MedianHospitalizationDuration;
            MedianTimeToMortality         = pScenarioParameters.MedianTimeToMortality;
            MedianSerialInterval          = pScenarioParameters.MedianSerialInterval;
            TransmissibilityWindowToSI    = pScenarioParameters.TransmissibilityWindowToSI;

            DaysWithAggregates            = new List<DayWithAggregates>();
            RunUpDays                     = new List<DayADNCRunUp>();
        }

        /*
            Infer admissions data from daily absolute hospital bed occpuancy data, by assuming a
            continual discharge rate, (churn) based on median hospitalization duration.

            Store a high-res 5 day rolling window admissions with churn, along with a highly
            smoothed one for use later during cases-to-admissions autocalibration.
            
            (c.f.autocalibrateCasesPerAdmission() below).
        */
        private void ComputeAdmissionsWithChurnAndPopulateDaysWithAggregates()
        {
            foreach (DayRolling rolling in DaysRolling)
            {
                var day = new DayWithAggregates(this, rolling);

                if (rolling.Raw.TimelineIndex == 0) { DaysWithAggregates.Add(day); continue; }

                int deltaH       = (int)Math.Round(
                    rolling.Rolling5DayHospitalizations
                  - DaysRolling[rolling.Raw.TimelineIndex - 1].Rolling5DayHospitalizations
                );

                int deltaHSmooth = (int)Math.Round(
                    rolling.Rolling101DayHospitalizations
                  - DaysRolling[rolling.Raw.TimelineIndex - 1].Rolling101DayHospitalizations
                );

                int churn       = 0;
                int churnSmooth = 0;

                if (rolling.Raw.TimelineIndex < MedianHospitalizationDuration)
                {
                    /*
                        Assume flat prior occupancy during the run-up lag window.
                    */
                    churn       = (int)Math.Round(
                        rolling.Rolling5DayHospitalizations
                      / MedianHospitalizationDuration
                    );

                    churnSmooth = (int)Math.Round(
                        rolling.Rolling101DayHospitalizations
                      / MedianHospitalizationDuration
                    );
                }
                else
                {
                    /*
                        Account for prior buildup or decline of occupancy levels during the unit
                        median duration window prior to the day, by taking:
                        
                        (1 / m) * (integral (h / m) dt)
                        
                        where m is median duration, h is occupancy and t is time.
                    */
                    churn       = (int)Math.Round(
                        DaysRolling.Where(
                            recent => (
                                (recent.Raw.TimelineIndex >=
                                    rolling.Raw.TimelineIndex
                                  - MedianHospitalizationDuration
                                )
                            &&  (recent.Raw.TimelineIndex <= rolling.Raw.TimelineIndex)
                            )
                        ).Sum(integrand => integrand.Rolling5DayHospitalizations)
                      / Math.Pow(MedianHospitalizationDuration, 2)
                    );

                    churnSmooth = (int)Math.Round(
                        DaysRolling.Where(
                            recent => (
                                (recent.Raw.TimelineIndex >=
                                    rolling.Raw.TimelineIndex
                                  - MedianHospitalizationDuration
                                )
                            &&  (recent.Raw.TimelineIndex <= rolling.Raw.TimelineIndex)
                            )
                        ).Sum(integrand => integrand.Rolling101DayHospitalizations)
                      / Math.Pow(MedianHospitalizationDuration, 2)
                    );
                }

                day.AdmissionsWithChurn       = Math.Max(deltaH       + churn       ,0);
                day.AdmissionsWithChurnSmooth = Math.Max(deltaHSmooth + churnSmooth ,0);

                DaysWithAggregates.Add(day);
            }

            /*
                The first day had no delta, and is back-filled.
            */
            DaysWithAggregates[0].AdmissionsWithChurn
                = DaysWithAggregates[1].AdmissionsWithChurn;

            DaysWithAggregates[0].AdmissionsWithChurnSmooth
                = DaysWithAggregates[1].AdmissionsWithChurnSmooth;
        }

        /*
            Compute historical transmission for the window of lag from a single median
            time-to-mortality up until the first day of source data, by simple extrapolation
            from mortality and IFR.
        */
        private void ComputeAndPopulateRunUpDays()
        {
            int i = 0;

            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                if (i >= MedianTimeToMortality) { break; }

                RunUpDays.Add(
                    new DayADNCRunUp(
                         i - MedianTimeToMortality
                        ,DaysRaw[0].Date.AddDays(i - MedianTimeToMortality)
                    )
                );

                RunUpDays[i].ActualDNC = (int)Math.Round(
                    day.Rolling.Rolling5DayMortality / IFR
                );

                i++;
            }
        }

        /*
            Mark the source data type for the various time regions of the model.
        */
        private void ComputeActualDNCSourcedOn()
        {
            int i = 0;

            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                if      (i < DaysWithAggregates.Count - MedianTimeToMortality)
                {
                    day.SourcedOn = SourcedOn.MortalityAndAdmissions;
                }
                else if (i < DaysWithAggregates.Count - MedianTimeToAdmission)
                {
                    day.SourcedOn = SourcedOn.AdmissionsOnly;
                }
                else if (i < DaysWithAggregates.Count - MedianTimeFromExposureToTest)
                {
                    day.SourcedOn = SourcedOn.TestResults;
                }
                else
                {
                    day.SourcedOn = SourcedOn.ProjectedCases;
                }

                i++;
            }
        }

        /*
            Compensate for the fact that cases per admission is dependent on healthcare capcity
            pressures during different epidemic phases, by continually re-anchoring to mortality
            and IFR. A wide rolling window is selected for autocalibration, so as not to lose
            the higher time-resolution signal from admissions data, relative to the more objective
            but lower-resolution higher-variance one from mortality.
        */
        private void AutocalibrateCasesPerAdmission()
        {
            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                int mortalityWhen  = day.Raw.TimelineIndex + MedianTimeToMortality;
                int admissionsWhen = day.Raw.TimelineIndex + MedianTimeToAdmission;

                if (
                    (day.SourcedOn == SourcedOn.MortalityAndAdmissions)
                &&  (DaysWithAggregates[admissionsWhen].AdmissionsWithChurnSmooth > 0)
                ) {
                    day.CasesPerAdmission = (int)Math.Round(
                        DaysWithAggregates[mortalityWhen].Rolling.Rolling101DayMortality
                      / (IFR * DaysWithAggregates[admissionsWhen].AdmissionsWithChurnSmooth)
                    );
                }

                if (day.CasesPerAdmission < 5) { day.CasesPerAdmission = 5; }
            }

            int finalCasesPerAdmission = DaysWithAggregates.Where(
                day => day.SourcedOn == SourcedOn.MortalityAndAdmissions
            ).Last().CasesPerAdmission;

            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                if (day.SourcedOn == SourcedOn.AdmissionsOnly)
                {
                    day.CasesPerAdmission = finalCasesPerAdmission;
                }
            }
        }

        /*
            Interior model region transmission.
        
            Compute transmission for the model region based on mortality and admissions via IFR
            and cases-to-admissions, (simple mean). Compute transmission for the model region
            based on admissions only via cases-to-admissions (simple extrapolation).

            Compute scaled-up transmission for the model region that is based on reported test
            results only. We compensate for implicit dependence of the scaling factor on variation
            in deployed test capacity and test positivity, by assuming the existence of a locally
            linear Taylor-series-like expansion, that yields correlation coefficients between
            positivity and transmission, and deployed capacity and transmission respectively.
        */
        private void ComputeActualDNCWithoutProjection()
        {
            bool   testHandoverPerformed   = false;
            double dncAutoCalibrationScale = 0;
            double testPositivityAtTZero   = 0;
            double testDeploymentAtTZero   = 0;

            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                if (day.SourcedOn == SourcedOn.MortalityAndAdmissions)
                {
                    double casesByMortality  = (
                        DaysRolling[
                            day.Raw.TimelineIndex + MedianTimeToMortality
                        ].Rolling5DayMortality
                      / IFR
                    );

                    double casesByAdmissions = (
                        DaysWithAggregates[
                            day.Raw.TimelineIndex + MedianTimeToAdmission
                        ].AdmissionsWithChurn
                      * day.CasesPerAdmission
                    );

                    day.ActualDNC = (int)Math.Round(
                        (casesByMortality + casesByAdmissions) / 2
                    );
                }

                if (day.SourcedOn == SourcedOn.AdmissionsOnly)
                {
                    day.ActualDNC = (
                        DaysWithAggregates[
                            day.Raw.TimelineIndex + MedianTimeToAdmission
                        ].AdmissionsWithChurn
                      * day.CasesPerAdmission
                    );
                }

                if (day.SourcedOn == SourcedOn.TestResults)
                {
                    if (!testHandoverPerformed)
                    {
                        int minIndex = (
                            (day.Raw.TimelineIndex - 3) > 0
                        ) ? (day.Raw.TimelineIndex - 3) : 0;

                        List<DayWithAggregates> adncDays = DaysWithAggregates.Where(
                            adncDay => (
                                adncDay.Raw.TimelineIndex >= minIndex
                            &&  adncDay.Raw.TimelineIndex  < day.Raw.TimelineIndex
                            )
                        ).ToList();

                        double n = (double)adncDays.Count;

                        int maxIndex = (
                            (day.Raw.TimelineIndex + 2) < (DaysWithAggregates.Count - 1)
                        ) ? (day.Raw.TimelineIndex + 2) : (DaysWithAggregates.Count - 1);

                        List<DayWithAggregates> dncDays = DaysWithAggregates.Where(
                            dncDay => (
                                dncDay.Raw.TimelineIndex >= day.Raw.TimelineIndex
                            &&  dncDay.Raw.TimelineIndex <= maxIndex
                            )
                        ).ToList();

                        double m = (double)dncDays.Count;

                        dncAutoCalibrationScale =
                            (adncDays.Select(adncDay => adncDay.ActualDNC            ).Sum() / n)
                          / (dncDays.Select( dncDay  => dncDay.Rolling.Rolling5DayDNC).Sum() / m);

                        testPositivityAtTZero = DaysRolling[
                            day.Raw.TimelineIndex
                        ].Rolling5DayPositivity;

                        testDeploymentAtTZero = DaysRolling[
                            day.Raw.TimelineIndex
                        ].Rolling5DayTests;

                        testHandoverPerformed = true;
                    }

                    day.ActualDNC = (int)Math.Round(
                        (day.Rolling.Rolling5DayPositivity / testPositivityAtTZero)
                      * (testDeploymentAtTZero             / day.Rolling.Rolling5DayTests)
                      * dncAutoCalibrationScale
                      * day.Rolling.Rolling5DayDNC
                    );
                }
            }
        }

        /*
            Extract time-local growth rate and R-Eff.

            The formula for R-Eff aggregation is a first order approximation appropriate only for
            growth values > -0.35 below which we set R-Eff to zero.
        */
        private void ComputeDeltasWithoutProjection()
        {
            int daysWithoutProjection  = DaysWithAggregates.Where(
                day => day.SourcedOn  != SourcedOn.ProjectedCases
            ).Count();

            foreach (DayWithAggregates day in DaysWithAggregates)
            {
                if (day.SourcedOn == SourcedOn.ProjectedCases) { continue; }

                int minIndex = (
                    (day.Raw.TimelineIndex - 4) > 0
                ) ? (day.Raw.TimelineIndex - 4) : 0;

                int maxIndex = (
                    (day.Raw.TimelineIndex + 4) < (daysWithoutProjection - 1)
                ) ? (day.Raw.TimelineIndex + 4) : (daysWithoutProjection - 1);

                List<DayWithAggregates> days = DaysWithAggregates.Where(
                    d => (d.Raw.TimelineIndex >= minIndex && d.Raw.TimelineIndex <= maxIndex)
                ).ToList();

                List<int> deltas = days.Zip(
                     days.Skip(1)
                    ,(current, next) => next.ActualDNC - current.ActualDNC
                ).ToList();

                day.Rolling9DayDeltaCDeltaT = (
                    (double)deltas.Sum()
                  / (double)deltas.Count
                );

                if (day.ActualDNC == 0)
                {
                    day.GrowthRate = 0;
                }
                else
                {
                    day.GrowthRate
                        = day.Rolling9DayDeltaCDeltaT / day.ActualDNC;
                }

                if (!
                    (
                        (
                            day.GrowthRate >= -0.9
                        &&  day.GrowthRate <= -0.02
                        )
                    ||  day.GrowthRate > 0.02
                    )
                ) {
                    day.DoublingTime = 0;
                } else {
                    day.DoublingTime
                        = Math.Log(2) / Math.Log(1 + day.GrowthRate);
                }

                day.REff = (
                    1
                  + (day.GrowthRate * MedianSerialInterval)
                  + (
                        (TransmissibilityWindowToSI)
                      * (1 - TransmissibilityWindowToSI)
                      * Math.Pow(
                           (day.GrowthRate * MedianSerialInterval)
                          ,2
                        )
                    )
                );

                if (day.REff < 0 || day.GrowthRate < -0.35)
                {
                    day.REff = 0;
                }
            }
        }

        /*
            Extract the most recently seen growth rate and R-Eff, and retrodict transmission for
            the final model region, (namely the window of lag from a single median
            time-from-exposure-to-test up until the most recent day), via exponential function
            interpolation.
        */
        private void ExtractGlobalGrowthAggregatesAndCastProjection()
        {
            int maxIndex   = DaysWithAggregates.Where(
                day => day.SourcedOn != SourcedOn.ProjectedCases
            ).Count() - 1;

            int minIndex   = (
                (maxIndex - 2) > 0
            ) ? (maxIndex - 2) : 0;

            List<double> recentGrowth = DaysWithAggregates.Where(
                day => (
                    day.Raw.TimelineIndex >= minIndex
                &&  day.Raw.TimelineIndex <= maxIndex
                )
            ).Select(day => day.GrowthRate).ToList();

            List<double> recentREff   = DaysWithAggregates.Where(
                day => (
                    day.Raw.TimelineIndex >= minIndex
                &&  day.Raw.TimelineIndex <= maxIndex
                )
            ).Select(day => day.REff).ToList();

            CurrentGrowthRate         = (
                recentGrowth.Sum() / recentGrowth.Count
            );

            CurrentREff               = (decimal)Math.Round(
                 (recentREff.Sum() / recentREff.Count)
                ,2
            );

            DaysWithAggregates.Where(
                day => day.SourcedOn == SourcedOn.ProjectedCases
            ).ToList().ForEach(day => {
                day.REff = (double)CurrentREff;
            });

            int i       = 1;
            int DNCZero = DaysWithAggregates.Where(
                day => day.SourcedOn == SourcedOn.TestResults
            ).Last().ActualDNC;

            DaysWithAggregates.Where(
                day => day.SourcedOn == SourcedOn.ProjectedCases
            ).ToList().ForEach(pday => {
                pday.ActualDNC = (int)Math.Round(
                    DNCZero * Math.Exp(CurrentGrowthRate * i)
                );

                i++;
            });
        }

        /*
            Compute cumulative seroprevalence and mortality as straightfoward sums/integrands.
        */
        private void ExtractGlobalLinearAggregates()
        {
            int totalCumalativeMortality = DaysRaw.Sum(raw => raw.Mortality);

            int totalCases
              = RunUpDays.Sum(         day => day.ActualDNC)
              + DaysWithAggregates.Sum(day => day.ActualDNC);

            ProjectedTotalSeroprev = (decimal)Math.Round(
                 (100 * ((double)totalCases / (double)AllScenarios.Population))
                ,1
            );

            var currentMortality = new List<int>();

            ProjectedTotalMortality = totalCumalativeMortality + (int)Math.Round(
                DaysWithAggregates.Where(
                    day => (
                        day.SourcedOn == SourcedOn.AdmissionsOnly
                    ||  day.SourcedOn == SourcedOn.TestResults
                    ||  day.SourcedOn == SourcedOn.ProjectedCases
                    )
                ).Sum(day => day.ActualDNC)
              * IFR
            );
        }

        /*
            Run the scenario.
        */
        public void RunScenario() {
            ComputeAdmissionsWithChurnAndPopulateDaysWithAggregates();
            ComputeAndPopulateRunUpDays();
            ComputeActualDNCSourcedOn();
            AutocalibrateCasesPerAdmission();
            ComputeActualDNCWithoutProjection();
            ComputeDeltasWithoutProjection();
            ExtractGlobalGrowthAggregatesAndCastProjection();
            ExtractGlobalLinearAggregates();
        }
    }
}

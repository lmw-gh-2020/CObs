using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
    public class ResultsDay
    {
        public int        TimelineIndex                     { get; set; }
        public DateTime   Date                              { get; set; }

        public int        Mortality                         { get; set; }
        public int        Hospitalizations                  { get; set; }
        public int        Tests                             { get; set; }
        public double     Positivity                        { get; set; }

        public double     Rolling5DayMortality              { get; set; }
        public double     Rolling5DayHospitalizations       { get; set; }
        public double     Rolling5DayTests                  { get; set; }
        public double     Rolling5DayPositivity             { get; set; }

        public SourcedOn  LowerBoundSourcedOn               { get; set; }
        public SourcedOn  BaselineSourcedOn                 { get; set; }
        public SourcedOn  UpperBoundSourcedOn               { get; set; }

        public int        AdmissionsWithChurnLowerBound     { get; set; }
        public int        AdmissionsWithChurnBaseline       { get; set; }
        public int        AdmissionsWithChurnUpperBound     { get; set; }

        public int        ActualDNCLowerBound               { get; set; }
        public int        ActualDNCBaseline                 { get; set; }
        public int        ActualDNCUpperBound               { get; set; }

        public double     Rolling9DayDeltaCDeltaTLowerBound { get; set; }
        public double     Rolling9DayDeltaCDeltaTBaseline   { get; set; }
        public double     Rolling9DayDeltaCDeltaTUpperBound { get; set; }

        /* (+ve/-ve) infinite growth rate, (i.e. at zero incidence) is stored as zero */
        public double     GrowthRateLowerBound              { get; set; }
        public double     GrowthRateBaseline                { get; set; }
        public double     GrowthRateUpperBound              { get; set; }

        /* (+ve/-ve) infinite doubling time, (i.e. at R-eff = 1) is stored as zero */
        public double     REffLowerBound                    { get; set; }
        public double     REffBaseline                      { get; set; }
        public double     REffUpperBound                    { get; set; }

        public double     DoublingTimeLowerBound            { get; set; }
        public double     DoublingTimeBaseline              { get; set; }
        public double     DoublingTimeUpperBound            { get; set; }

        public ResultsDay(int pTimelineIndex)
        {
            TimelineIndex       = pTimelineIndex;

            BaselineSourcedOn   = SourcedOn.Unknown;
            LowerBoundSourcedOn = SourcedOn.Unknown;
            UpperBoundSourcedOn = SourcedOn.Unknown;
        }
    }

    public class Aggregates
    {
        /* global growth aggregates */
        public decimal CurrentREffLowerBound             { get; set; }
        public decimal CurrentREffBaseline               { get; set; }
        public decimal CurrentREffUpperBound             { get; set; }
        public int     CurrentDoublingTimeLowerBound     { get; set; }
        public int     CurrentDoublingTimeBaseline       { get; set; }
        public int     CurrentDoublingTimeUpperBound     { get; set; }

        public bool    CurrentDoublingTimeUnstable       { get; set; }

        /* global linear aggregates */
        public decimal ProjectedTotalSeroprevLowerBound  { get; set; }
        public decimal ProjectedTotalSeroprevBaseline    { get; set; }
        public decimal ProjectedTotalSeroprevUpperBound  { get; set; }
        public int     ProjectedTotalMortalityLowerBound { get; set; }
        public int     ProjectedTotalMortalityBaseline   { get; set; }
        public int     ProjectedTotalMortalityUpperBound { get; set; }
    }

    public class Builder
    {
        public ICommitEventsAsync CommitAdapter { get; private set; }
        public BaseDays           BaseDays      { get; private set; }
        public AllScenarios       Scenarios     { get; private set; }

        private List<ResultsDay>  ResultsDays   { get; set; }
        private Aggregates        Aggregates    { get; set; }
        private ulong             ReadPosition  { get; set; }

        public Builder(
             ICommitEventsAsync pCommitAdapter
            ,AllScenarios       pScenarios
        ) {
            CommitAdapter = pCommitAdapter;
            BaseDays      = pScenarios.BaseDays;
            Scenarios     = pScenarios;

            ResultsDays   = new List<ResultsDay>();
            Aggregates    = new Aggregates();
            ReadPosition = 0;
        }

        public async Task<ICommitActionResult> RegisterBuild()
        {
            return await CommitAdapter.RegisterBuild();
        }

        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            var readResult = await BaseDays.ReadDaysRawAsync();

            if (readResult.Success) { ReadPosition = readResult.ReadPosition; }

            return readResult;
        }

        public void PopulateRolling()
        {
            BaseDays.PopulateRolling();
        }

        public void GenerateScenarios()
        {
            Scenarios.GenerateScenarioParameters();
        }

        public void RunScenarios()
        {
            foreach (var scenario in Scenarios.Scenarios)
            {
                scenario.RunScenario();
            }
        }

        /*
            Extract low, baseline and high transmission values for each day, (along with their
            associated local aggregates) by inspecting the extremal amd median scenarios for that
            day.
        */
        public void ExtractResultDays()
        {
            int minTimelineIndex = Scenarios.Scenarios.SelectMany(
                scenario => scenario.RunUpDays
            ).Select(day => day.TimelineIndex).Min();

            int maxTimelineIndex = BaseDays.DaysRaw.Select(
                day => day.TimelineIndex
            ).Max();

            int elements = 0;
            int median   = 0;

            bool foundMaxElements = false;

            for (int i = minTimelineIndex; i <= maxTimelineIndex; i++)
            {
                var resultDay = new ResultsDay(i);

                if (resultDay.TimelineIndex < 0)
                {
                    /*
                        The size of the set of scenarios having data is variable during the run-up
                        period.
                    */
                    resultDay.Date        = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.RunUpDays
                    ).Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).First().Date;

                    elements              = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.RunUpDays
                    ).Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).Count();

                    median                = (int)Math.Floor(elements / 2.0M);

                    DayADNCRunUp lower    = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.RunUpDays
                    ).Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).OrderBy(
                        day      => day.ActualDNC
                    ).First();

                    DayADNCRunUp baseline = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.RunUpDays
                    ).Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).OrderBy(
                        day      => day.ActualDNC
                    ).Skip(median).Take(1).First();

                    DayADNCRunUp upper    = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.RunUpDays
                    ).Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).OrderByDescending(
                        day      => day.ActualDNC
                    ).First();

                    resultDay.LowerBoundSourcedOn = lower.SourcedOn;
                    resultDay.BaselineSourcedOn   = baseline.SourcedOn;
                    resultDay.UpperBoundSourcedOn = upper.SourcedOn;

                    resultDay.ActualDNCLowerBound = lower.ActualDNC;
                    resultDay.ActualDNCBaseline   = baseline.ActualDNC;
                    resultDay.ActualDNCUpperBound = upper.ActualDNC;
                }
                else
                {
                    /*
                        The maximal size of the set of scenarios having data has now been reached.
                    */
                    if (!foundMaxElements)
                    {
                        elements               = Scenarios.Scenarios.SelectMany(
                            scenario => scenario.DaysWithAggregates
                        ).Where(
                            day      => day.Raw.TimelineIndex == resultDay.TimelineIndex
                        ).Count();

                        median                 = (int)Math.Floor(elements / 2.0M);

                        foundMaxElements       = true;
                    }

                    resultDay.Date             = BaseDays.DaysRaw.Where(
                        day      => day.TimelineIndex == resultDay.TimelineIndex
                    ).First().Date;

                    DayWithAggregates lower    = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.DaysWithAggregates
                    ).Where(
                        day      => day.Raw.TimelineIndex == resultDay.TimelineIndex
                    ).OrderBy(
                        day      => day.ActualDNC
                    ).First();

                    DayWithAggregates baseline = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.DaysWithAggregates
                    ).Where(
                        day      => day.Raw.TimelineIndex == resultDay.TimelineIndex
                    ).OrderBy(
                        day      => day.ActualDNC
                    ).Skip(median).Take(1).First();

                    DayWithAggregates upper    = Scenarios.Scenarios.SelectMany(
                        scenario => scenario.DaysWithAggregates
                    ).Where(
                        day      => day.Raw.TimelineIndex == resultDay.TimelineIndex
                    ).OrderByDescending(
                        day      => day.ActualDNC
                    ).First();

                    resultDay.Mortality                         = BaseDays.DaysRaw[
                        resultDay.TimelineIndex
                    ].Mortality;

                    resultDay.Hospitalizations                  = BaseDays.DaysRaw[
                        resultDay.TimelineIndex
                    ].Hospitalizations;

                    resultDay.Tests                             = BaseDays.DaysRaw[
                        resultDay.TimelineIndex
                    ].Tests;

                    resultDay.Positivity                        = BaseDays.DaysRaw[
                        resultDay.TimelineIndex
                    ].Positivity;

                    resultDay.Rolling5DayMortality              = BaseDays.DaysRolling[
                        resultDay.TimelineIndex
                    ].Rolling5DayMortality;

                    resultDay.Rolling5DayHospitalizations       = BaseDays.DaysRolling[
                        resultDay.TimelineIndex
                    ].Rolling5DayHospitalizations;

                    resultDay.Rolling5DayTests                  = BaseDays.DaysRolling[
                        resultDay.TimelineIndex
                    ].Rolling5DayTests;

                    resultDay.Rolling5DayPositivity             = BaseDays.DaysRolling[
                        resultDay.TimelineIndex
                    ].Rolling5DayPositivity;

                    resultDay.LowerBoundSourcedOn               = lower.SourcedOn;
                    resultDay.BaselineSourcedOn                 = baseline.SourcedOn;
                    resultDay.UpperBoundSourcedOn               = upper.SourcedOn;

                    resultDay.AdmissionsWithChurnLowerBound     = lower.AdmissionsWithChurn;
                    resultDay.AdmissionsWithChurnBaseline       = baseline.AdmissionsWithChurn;
                    resultDay.AdmissionsWithChurnUpperBound     = upper.AdmissionsWithChurn;

                    resultDay.ActualDNCLowerBound               = lower.ActualDNC;
                    resultDay.ActualDNCBaseline                 = baseline.ActualDNC;
                    resultDay.ActualDNCUpperBound               = upper.ActualDNC;

                    resultDay.Rolling9DayDeltaCDeltaTLowerBound = lower.Rolling9DayDeltaCDeltaT;
                    resultDay.Rolling9DayDeltaCDeltaTBaseline   = baseline.Rolling9DayDeltaCDeltaT;
                    resultDay.Rolling9DayDeltaCDeltaTUpperBound = upper.Rolling9DayDeltaCDeltaT;

                    resultDay.GrowthRateLowerBound              = lower.GrowthRate;
                    resultDay.GrowthRateBaseline                = baseline.GrowthRate;
                    resultDay.GrowthRateUpperBound              = upper.GrowthRate;

                    resultDay.REffLowerBound                    = lower.REff;
                    resultDay.REffBaseline                      = baseline.REff;
                    resultDay.REffUpperBound                    = upper.REff;

                    resultDay.DoublingTimeLowerBound            = lower.DoublingTime;
                    resultDay.DoublingTimeBaseline              = baseline.DoublingTime;
                    resultDay.DoublingTimeUpperBound            = upper.DoublingTime;
                }

                ResultsDays.Add(resultDay);
            }
        }

        /*
            Extract low, baseline and high values for both cumulative seroprevalence and mortality,
            and most recent growth rate and R-Eff, by inspecting the extremal amd median scenarios
            for the most recent day.
        */
        public void ExtractAggregates()
        {
            var currentMortality  = new List<int>();
            var currentSeroPrev   = new List<decimal>();
            var currentGrowthRate = new List<double>();
            var currentREff       = new List<decimal>();

            int           maxTimelineIndex  = BaseDays.DaysRaw.Select(
                day      => day.TimelineIndex
            ).Max();

            int           elements          = Scenarios.Scenarios.SelectMany(
                scenario => scenario.DaysWithAggregates
            ).Where(
                day      => day.Raw.TimelineIndex == maxTimelineIndex
            ).Count();

            int           median            = (int)Math.Floor(elements / 2.0M);

            Scenario      lower             = Scenarios.Scenarios.SelectMany(
                scenario => scenario.DaysWithAggregates
            ).Where(
                day      => day.Raw.TimelineIndex == maxTimelineIndex
            ).OrderBy(
                day      => day.ActualDNC
            ).First().Parent;

            Scenario      baseline          = Scenarios.Scenarios.SelectMany(
                scenario => scenario.DaysWithAggregates
            ).Where(
                day      => day.Raw.TimelineIndex == maxTimelineIndex
            ).OrderBy(
                day      => day.ActualDNC
            ).Skip(median).Take(1).First().Parent;

            Scenario      upper             = Scenarios.Scenarios.SelectMany(
                scenario => scenario.DaysWithAggregates
            ).Where(
                day      => day.Raw.TimelineIndex == maxTimelineIndex
            ).OrderByDescending(
                day      => day.ActualDNC
            ).First().Parent;

            currentMortality.Add(lower.ProjectedTotalMortality);
            currentMortality.Add(baseline.ProjectedTotalMortality);
            currentMortality.Add(upper.ProjectedTotalMortality);

            currentSeroPrev.Add(lower.ProjectedTotalSeroprev);
            currentSeroPrev.Add(baseline.ProjectedTotalSeroprev);
            currentSeroPrev.Add(upper.ProjectedTotalSeroprev);

            currentGrowthRate.Add(lower.CurrentGrowthRate);
            currentGrowthRate.Add(baseline.CurrentGrowthRate);
            currentGrowthRate.Add(upper.CurrentGrowthRate);

            currentREff.Add(lower.CurrentREff);
            currentREff.Add(baseline.CurrentREff);
            currentREff.Add(upper.CurrentREff);

            /*
                Note that high transmission and prevalence are not necessarily correlated with
                high mortality, as they may be an artifact of relatively low IFR. Also note that
                the correlation between transmission and growth rate/R-Eff is generally inverse
                in epidemic decline phases.

                Therefore no assumption is made as to the ordering of the extracted aggregates
                with respect to whether the parent scenario is low, baseline, or high transmission.

                (We could independently search the parameter space for extremal and median values
                if we wished to refine this).
            */
            List<int>     rangedMortality = currentMortality.OrderBy( r => r).ToList();
            List<decimal> rangedSeroPrev  = currentSeroPrev.OrderBy(  r => r).ToList();
            List<double>  rangedGrowth    = currentGrowthRate.OrderBy(r => r).ToList();
            List<decimal> rangedREff      = currentREff.OrderBy(      r => r).ToList();

            Aggregates.ProjectedTotalMortalityLowerBound = rangedMortality[0];
            Aggregates.ProjectedTotalMortalityBaseline   = rangedMortality[1];
            Aggregates.ProjectedTotalMortalityUpperBound = rangedMortality[2];

            Aggregates.ProjectedTotalSeroprevLowerBound  = rangedSeroPrev[0];
            Aggregates.ProjectedTotalSeroprevBaseline    = rangedSeroPrev[1];
            Aggregates.ProjectedTotalSeroprevUpperBound  = rangedSeroPrev[2];

            double growthRateLowerBound                  = rangedGrowth[0];
            double growthRateBaseline                    = rangedGrowth[1];
            double growthRateUpperBound                  = rangedGrowth[2];

            Aggregates.CurrentREffLowerBound             = Math.Round(rangedREff[0] ,2);
            Aggregates.CurrentREffBaseline               = Math.Round(rangedREff[1] ,2);
            Aggregates.CurrentREffUpperBound             = Math.Round(rangedREff[2] ,2);

            if (!
                (
                    (
                        growthRateLowerBound >= -0.9
                    &&  growthRateLowerBound <= -0.02
                    )
                ||  growthRateLowerBound > 0.02
                )
            ) {
                Aggregates.CurrentDoublingTimeLowerBound = 0;
            } else {
                Aggregates.CurrentDoublingTimeLowerBound = (int)Math.Round(
                    Math.Log(2) / Math.Log(1 + growthRateLowerBound)
                );
            }

            if (!
                (
                    (
                        growthRateBaseline >= -0.9
                    &&  growthRateBaseline <= -0.02
                    )
                ||  growthRateBaseline > 0.02
                )
            ) {
                Aggregates.CurrentDoublingTimeBaseline = 0;
            } else {
                Aggregates.CurrentDoublingTimeBaseline = (int)Math.Round(
                    Math.Log(2) / Math.Log(1 + growthRateBaseline)
                );
            }

            if (!
                (
                    (
                        growthRateUpperBound >= -0.9
                    &&  growthRateUpperBound <= -0.02
                    )
                ||  growthRateUpperBound > 0.02
                )
            ) {
                Aggregates.CurrentDoublingTimeUpperBound = 0;
            } else {
                Aggregates.CurrentDoublingTimeUpperBound = (int)Math.Round(
                    Math.Log(2) / Math.Log(1 + growthRateUpperBound)
                );
            }

            if (
                (!(
                        Aggregates.CurrentREffLowerBound     >= 1.1m
                    ||  Aggregates.CurrentREffUpperBound     <= 0.9m
                ))
                ||  Aggregates.CurrentDoublingTimeLowerBound == 0
                ||  Aggregates.CurrentDoublingTimeBaseline   == 0
                ||  Aggregates.CurrentDoublingTimeUpperBound == 0
            ) {
                Aggregates.CurrentDoublingTimeUnstable = true;
            }
        }

        public async Task<ICommitActionResult> CommitResultsAsync()
        {
            return await CommitAdapter.CommitResultsAsync(
                 ResultsDays ,Aggregates ,ReadPosition
            );
        }
    }
}

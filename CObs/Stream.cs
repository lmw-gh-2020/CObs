using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using EventStore.Client;

namespace CObs
{
    public interface IReadActionResult
    {
        bool                   Success { get; }
        string                 Message { get; }
        SourceValidationStatus Status  { get; }
        List<DayRaw>           DaysRaw { get; }
    }

    public interface IReadEventsAsync
    {
        Task<IReadActionResult> ReadDaysRawAsync();
    }

    public interface ICommitActionResult
    {
        bool   Success { get; }
        string Message { get; }
    }

    public interface ICommitEvents
    {
        ICommitActionResult CommitResults(List<ResultsDay> pResultsDays);
        ICommitActionResult CommitAggregates(Aggregates pAggregates);
    }

    class ReadActionResult : IReadActionResult
    {
        public bool                   Success { get; private set; }
        public string                 Message { get; private set; }
        public SourceValidationStatus Status  { get; private set; }
        public List<DayRaw>           DaysRaw { get; private set; }

        public ReadActionResult(
             bool                   pSuccess
            ,string                 pMessage
            ,SourceValidationStatus pStatus
            ,List<DayRaw>           pDaysRaw
        ) {
            Success = pSuccess;
            Message = pSuccess ? ""       : pMessage;
            Status  = pSuccess
                ? new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                : pStatus;
            DaysRaw = pSuccess ? pDaysRaw : new List<DayRaw>();
        }
    }

    class CommitActionResult : ICommitActionResult
    {
        public bool   Success { get; private set; }
        public string Message { get; private set; }

        public CommitActionResult(
             bool         pSuccess
            ,string       pMessage
        ) {
            Success = pSuccess;
            Message = pSuccess ? "" : pMessage;
        }
    }

    public class SourceFromFile : IReadEventsAsync
    {
        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            int       index    = 0;
            DateTime? lastDate = null;
            var       daysRaw  = new List<DayRaw>();

            try
            {
                /* read raw source data */
                using var sr = new StreamReader("SourceData.txt");
                
                string? line;

                while ((line = await sr.ReadLineAsync()) != null)
                {
                    List<string> row = line.Split(',').ToList().Select(
                        entry => entry.Trim()
                    ).ToList();

                    SourceRowValidationStatus status = BaseDays.ValidateSourceRow(
                         row
                        ,lastDate
                    );

                    if (status != SourceRowValidationStatus.OK)
                    {
                        /* report validation error */
                        return new ReadActionResult(
                             false
                            ,"row validation error"
                            ,new SourceValidationStatus(
                                 false
                                ,(index + 1) /* line number hints start from 1 */
                                ,status
                            )
                            ,new List<DayRaw>()
                        );
                    }

                    var day = new DayRaw(index, row);

                    daysRaw.Add(day);

                    lastDate = day.Date;

                    index++;
                }                
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,new List<DayRaw>()
                );
            }

            /* return souce events */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,daysRaw
            );
        }
    }

    public class ResultsToFile : ICommitEvents
    {
        public ICommitActionResult CommitResults(List<ResultsDay> pResultsDays)
        {
            try
            {
                using (var w = new StreamWriter("ResultsData.txt"))
                {
                    foreach (ResultsDay day in pResultsDays)
                    {
                        string line
                          = day.TimelineIndex
                          + " ,"
                          + day.Date.ToString("yyyy-MM-dd")
                          + " ,"
                          + day.Mortality
                          + " ,"
                          + day.Hospitalizations
                          + " ,"
                          + day.Tests
                          + " ,"
                          + day.Positivity
                          + " ,"
                          + day.Rolling5DayMortality
                          + " ,"
                          + day.Rolling5DayHospitalizations
                          + " ,"
                          + day.Rolling5DayTests
                          + " ,"
                          + day.Rolling5DayPositivity
                          + " ,"
                          + (int)day.LowerBoundSourcedOn
                          + " ,"
                          + (int)day.BaselineSourcedOn
                          + " ,"
                          + (int)day.UpperBoundSourcedOn
                          + " ,"
                          + day.AdmissionsWithChurnLowerBound
                          + " ,"
                          + day.AdmissionsWithChurnBaseline
                          + " ,"
                          + day.AdmissionsWithChurnUpperBound
                          + " ,"
                          + day.ActualDNCLowerBound
                          + " ,"
                          + day.ActualDNCBaseline
                          + " ,"
                          + day.ActualDNCUpperBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTLowerBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTBaseline
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTUpperBound
                          + " ,"
                          + day.GrowthRateLowerBound
                          + " ,"
                          + day.GrowthRateBaseline
                          + " ,"
                          + day.GrowthRateUpperBound
                          + " ,"
                          + day.REffLowerBound
                          + " ,"
                          + day.REffBaseline
                          + " ,"
                          + day.REffUpperBound
                          + " ,"
                          + day.DoublingTimeLowerBound
                          + " ,"
                          + day.DoublingTimeBaseline
                          + " ,"
                          + day.DoublingTimeUpperBound;

                        w.WriteLine(line);
                        w.Flush();
                    }

                    w.Close();
                }

                File.Copy("ResultsData.txt", "CObsResults/ResultsData.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing results to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        public ICommitActionResult CommitAggregates(Aggregates pAggregates)
        {
            try
            {
                using (var w = new StreamWriter("Aggregates.txt"))
                {
                    string line
                      = pAggregates.CurrentREffLowerBound
                      + " ,"
                      + pAggregates.CurrentREffBaseline
                      + " ,"
                      + pAggregates.CurrentREffUpperBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeLowerBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeBaseline
                      + " ,"
                      + pAggregates.CurrentDoublingTimeUpperBound
                      + " ,"
                      + ((pAggregates.CurrentDoublingTimeUnstable) ? 1 : 0)
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevUpperBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityUpperBound;

                    w.WriteLine(line);
                    w.Flush();
                    w.Close();
                }

                File.Copy("Aggregates.txt", "CObsResults/Aggregates.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing aggregates to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }
    }

    public class SourceFromEvents : IReadEventsAsync
    {
        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            var daysRaw                   = new List<DayRaw>();
            int                  index    = 0;
            DateTime?            lastDate = null;
            List<ResolvedEvent>? events;

            try
            {
                var client = new EventStoreClient(
                    EventStoreClientSettings.Create(
                        "esdb://localhost:2113?tls=false"
                    )
                );

                var result = client.ReadStreamAsync(
                     Direction.Forwards
                    , "outbreak"
                    , StreamPosition.Start
                );

                events = await result.ToListAsync();
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,new List<DayRaw>()
                );
            }

            foreach (var sourceDay in events)
            {
                SourceDayRoot? day;

                try
                {
                    day = JsonSerializer.Deserialize<SourceDayRoot>(
                        Encoding.UTF8.GetString(sourceDay.Event.Data.ToArray())!
                    )!;
                }
                catch (Exception e)
                {
                    /* report type exception */
                    return new ReadActionResult(
                         false
                        ,e.Message
                        ,new SourceValidationStatus(
                             false
                            ,(index + 1) /* line number hints start from 1 */
                            ,SourceRowValidationStatus.ExceptionReadingEvents
                        )
                        ,new List<DayRaw>()
                    );
                }

                SourceRowValidationStatus status = BaseDays.ValidateSourceEvent(
                     day.sourceDay!
                    ,lastDate
                );

                if (status != SourceRowValidationStatus.OK)
                {
                    /* report validation error */
                    return new ReadActionResult(
                         false
                        ,"row validation error"
                        ,new SourceValidationStatus(
                             false
                            ,(index + 1) /* line number hints start from 1 */
                            ,status
                        )
                        ,new List<DayRaw>()
                    );
                }

                var dayRaw = new DayRaw(index, day.sourceDay!);

                daysRaw.Add(dayRaw);

                lastDate = dayRaw.Date;

                index++;
            }

            /* return souce events */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,daysRaw
            );
        }
    }
}
